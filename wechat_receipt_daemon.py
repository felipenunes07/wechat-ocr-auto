#!/usr/bin/env python3
"""
WeChat receipt ingestion daemon (Windows-friendly).

Goal:
- Detect new image files continuously (no manual save click loop).
- Process with OCR.
- Extract date, time, beneficiary and amount.
- Append results to the configured sink with idempotency.
- Avoid missing files via periodic reconciliation scan.

Notes:
- There is no official webhook from WeChat Desktop local storage.
- This script emulates webhook behavior using filesystem events + durable queue.
"""

from __future__ import annotations

import argparse
import hashlib
import io
import json
import os
import re
import sqlite3
import sys
import threading
import time
import unicodedata
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path, PureWindowsPath
from typing import Any, Optional
from urllib.parse import parse_qs, urlparse

from PIL import Image, ImageFilter, ImageOps, ImageStat
from openpyxl import Workbook, load_workbook

try:
    from watchdog.events import FileSystemEventHandler
    from watchdog.observers import Observer

    WATCHDOG_AVAILABLE = True
except Exception:
    WATCHDOG_AVAILABLE = False
    class FileSystemEventHandler:  # type: ignore[override]
        pass
    class Observer:  # type: ignore[override]
        pass

try:
    from wechat_ui_force_download import UIMessageCandidate, WeChatUIForceDownloader

    UI_FORCE_DOWNLOADER_AVAILABLE = True
    UI_FORCE_DOWNLOADER_IMPORT_ERROR: Optional[str] = None
except Exception as exc:
    UIMessageCandidate = Any  # type: ignore[misc,assignment]
    WeChatUIForceDownloader = None  # type: ignore[assignment]
    UI_FORCE_DOWNLOADER_AVAILABLE = False
    UI_FORCE_DOWNLOADER_IMPORT_ERROR = f"{type(exc).__name__}: {exc}"


IMG_HEADERS: dict[str, tuple[int, int]] = {
    "jpg": (0xFF, 0xD8),
    "png": (0x89, 0x50),
    "gif": (0x47, 0x49),
    "webp": (0x52, 0x49),
}

IMG_SUFFIXES = {".jpg", ".jpeg", ".png", ".bmp", ".webp", ".gif", ".dat"}
PLAIN_IMAGE_SUFFIXES = {".jpg", ".jpeg", ".png", ".bmp", ".webp", ".gif"}
LANCZOS_FILTER = getattr(getattr(Image, "Resampling", Image), "LANCZOS")
BASE_LANC_HEADERS = [
    "CLIENTE",
    "DATA",
    "HORA",
    "BANCO",
    "VALOR",
]
DEFAULT_VERIFICATION_COLUMN_NAME = "STATUS_VERIFICACAO"


def is_candidate(path: Path) -> bool:
    if not path.is_file():
        return False
    if path.suffix.lower() not in IMG_SUFFIXES:
        return False

    s = str(path).lower().replace("/", "\\")

    if "\\msgattach\\" in s and "\\image\\" in s and path.suffix.lower() == ".dat":
        return True

    # WeChat can store full images in plain formats (.png/.jpg) under MsgAttach/Image.
    if "\\msgattach\\" in s and "\\image\\" in s and path.suffix.lower() in PLAIN_IMAGE_SUFFIXES:
        return True

    # Fallback lane to avoid losing incoming files when only thumbnail is available.
    if "\\msgattach\\" in s and "\\thumb\\" in s and path.suffix.lower() == ".dat":
        return True

    if "\\filestorage\\temp\\" in s and path.suffix.lower() in {".jpg", ".jpeg", ".png", ".bmp", ".webp"}:
        return True

    return False


def detect_source_kind(path: Path) -> str:
    s = str(path).lower().replace("/", "\\")
    if "\\msgattach\\" in s and "\\image\\" in s and path.suffix.lower() == ".dat":
        return "msgattach_image_dat"
    if "\\msgattach\\" in s and "\\thumb\\" in s and path.suffix.lower() == ".dat":
        return "msgattach_thumb_dat"
    if "\\filestorage\\temp\\" in s:
        return "temp_image"
    if "\\msgattach\\" in s and "\\image\\" in s:
        return "msgattach_image_plain"
    return "other"


def normalize_windows_text(value: str) -> str:
    return str(value or "").replace("/", "\\").strip().lower()


def path_to_normalized_windows(path: Path) -> str:
    return normalize_windows_text(str(path))


def build_lanc_headers(verification_column_name: str) -> list[str]:
    return [*BASE_LANC_HEADERS, verification_column_name.strip() or DEFAULT_VERIFICATION_COLUMN_NAME]


def sheet_header_range(headers: list[str]) -> str:
    last_col = chr(ord("A") + max(0, len(headers) - 1))
    return f"A1:{last_col}1"


def sheet_table_range(headers: list[str]) -> str:
    last_col = chr(ord("A") + max(0, len(headers) - 1))
    return f"A:{last_col}"


def resolve_full_image_from_thumb_path(thumb_path: Path) -> Optional[Path]:
    """Try to map MsgAttach/Thumb/<month>/<hash>_t.dat -> MsgAttach/Image/<month>/<hash>.(dat|jpg|png...)."""
    s = str(thumb_path).replace("/", "\\")
    if "\\msgattach\\" not in s.lower() or "\\thumb\\" not in s.lower():
        return None

    img_loc = s.replace("\\Thumb\\", "\\Image\\").replace("\\thumb\\", "\\Image\\")
    img_path = Path(img_loc)

    stem = img_path.stem
    base = stem[:-2] if stem.lower().endswith("_t") else stem
    candidates: list[Path] = []
    for ext in (".dat", ".jpg", ".jpeg", ".png", ".webp", ".bmp", ".gif"):
        candidates.append(img_path.with_name(f"{base}{ext}"))
    for ext in (".dat", ".jpg", ".jpeg", ".png", ".webp", ".bmp", ".gif"):
        candidates.append(img_path.with_name(f"{stem}{ext}"))

    for c in candidates:
        if c.exists() and c.is_file():
            return c
    return None


def expected_full_image_from_thumb_path(thumb_path: Path) -> Optional[Path]:
    s = str(thumb_path).replace("/", "\\")
    if "\\msgattach\\" not in s.lower() or "\\thumb\\" not in s.lower():
        return None

    img_loc = s.replace("\\Thumb\\", "\\Image\\").replace("\\thumb\\", "\\Image\\")
    img_path = Path(img_loc)
    stem = img_path.stem
    base = stem[:-2] if stem.lower().endswith("_t") else stem
    return img_path.with_name(f"{base}.dat")


def sha256_bytes(data: bytes) -> str:
    h = hashlib.sha256()
    h.update(data)
    return h.hexdigest()


def decode_wechat_dat(raw: bytes) -> tuple[bytes, str, int]:
    if len(raw) < 2:
        raise ValueError("Empty or invalid .dat file")

    for ext, (h0, h1) in IMG_HEADERS.items():
        k0 = raw[0] ^ h0
        k1 = raw[1] ^ h1
        if k0 != k1:
            continue
        key = k0
        decoded = bytes(b ^ key for b in raw)
        try:
            with Image.open(io.BytesIO(decoded)) as im:
                im.verify()
            return decoded, ext, key
        except Exception:
            continue

    raise ValueError("Unable to decode .dat as known image format")


def open_image_from_file(path: Path) -> tuple[Image.Image, bytes, str, Optional[int]]:
    raw = path.read_bytes()
    if path.suffix.lower() == ".dat":
        decoded, ext, key = decode_wechat_dat(raw)
        with Image.open(io.BytesIO(decoded)) as im:
            return im.convert("RGB"), decoded, ext, key
    with Image.open(io.BytesIO(raw)) as im:
        return im.convert("RGB"), raw, path.suffix.lower().lstrip("."), None


def quality_score(img: Image.Image) -> float:
    w, h = img.size
    gray = img.convert("L")
    var = float(ImageStat.Stat(gray).var[0])
    long_side = max(w, h)
    short_side = min(w, h)

    res_component = min(1.0, long_side / 1400.0) * 0.65 + min(1.0, short_side / 700.0) * 0.20
    contrast_component = min(1.0, var / 1800.0) * 0.15
    score = res_component + contrast_component
    return round(max(0.0, min(1.0, score)), 4)


class OCREngine:
    name = "none"

    def extract(self, img: Image.Image) -> tuple[str, float]:
        raise NotImplementedError


class RapidOCREngine(OCREngine):
    name = "rapidocr"

    def __init__(self) -> None:
        from rapidocr_onnxruntime import RapidOCR  # type: ignore

        self._ocr = RapidOCR()

    def extract(self, img: Image.Image) -> tuple[str, float]:
        import numpy as np  # type: ignore

        arr = np.array(img.convert("RGB"))
        result, _ = self._ocr(arr)
        if not result:
            return "", 0.0
        texts: list[str] = []
        confs: list[float] = []
        for item in result:
            if len(item) >= 3:
                texts.append(str(item[1]))
                try:
                    confs.append(float(item[2]))
                except Exception:
                    pass
        text = "\n".join(t for t in texts if t.strip())
        conf = (sum(confs) / len(confs)) if confs else 0.5
        return text, round(max(0.0, min(1.0, conf)), 4)


class TesseractOCREngine(OCREngine):
    name = "tesseract"

    def __init__(self) -> None:
        import pytesseract  # type: ignore

        cmd = os.getenv("TESSERACT_CMD", "").strip()
        if cmd:
            pytesseract.pytesseract.tesseract_cmd = cmd
        self._pytesseract = pytesseract
        self._lang = os.getenv("OCR_LANG", "por+eng+chi_sim")

    def extract(self, img: Image.Image) -> tuple[str, float]:
        text = self._pytesseract.image_to_string(img, lang=self._lang)
        text = text.strip()
        if not text:
            return "", 0.0
        return text, 0.55


def build_ocr_engine() -> OCREngine:
    try:
        return RapidOCREngine()
    except Exception:
        pass
    try:
        return TesseractOCREngine()
    except Exception:
        pass
    raise RuntimeError(
        "No OCR engine available. Install one:\n"
        "- pip install rapidocr-onnxruntime\n"
        "or\n"
        "- pip install pytesseract and install Tesseract OCR binary"
    )


DATE_PATTERNS = [
    re.compile(r"\b(\d{2}/\d{2}/\d{4})\b"),
    re.compile(r"\b(\d{4}-\d{2}-\d{2})\b"),
    re.compile(r"\b(\d{2}-\d{2}-\d{4})\b"),
    re.compile(r"\b(\d{2}/\d{2}/\d{2})\b"),
]
ALPHA_MONTH_DATE_PATTERN = re.compile(r"\b(\d{1,2})\s*[-/]?\s*([A-Za-z]{3})\s*[-/]?\s*(\d{4})\b", re.IGNORECASE)
TIME_PATTERN = re.compile(r"\b(\d{2}:\d{2}(?::\d{2})?)\b")
AMOUNT_CURRENCY_PATTERN = re.compile(
    r"(R\$?|RS|US\$|USD|BRL|CNY|RMB|¥|￥)\s*([0-9][0-9\.,]{0,20})",
    re.IGNORECASE,
)
AMOUNT_FALLBACK_PATTERN = re.compile(r"(?<!\d)([0-9]{1,3}(?:[\.,][0-9]{3})*[\.,][0-9]{2})(?!\d)")
MONTH_TOKEN_MAP = {
    "JAN": 1,
    "FEB": 2,
    "FEV": 2,
    "MAR": 3,
    "APR": 4,
    "ABR": 4,
    "MAY": 5,
    "MAI": 5,
    "JUN": 6,
    "JUL": 7,
    "AUG": 8,
    "AGO": 8,
    "SEP": 9,
    "SET": 9,
    "OCT": 10,
    "OUT": 10,
    "NOV": 11,
    "DEC": 12,
    "DEZ": 12,
}

BENEFICIARY_KEYS = [
    "favorecido",
    "beneficiario",
    "beneficiario",
    "destinatario",
    "destinatario",
    "recebedor",
    "recebedora",
    "nome",
    "recebido por",
    "para:",
    "收款方",
    "收款人",
    "对方",
]

BANK_ALLOWED = ("AMD", "DIAMOND", "CLEEND")


def normalize_text_for_match(value: str) -> str:
    value = unicodedata.normalize("NFKD", value)
    value = "".join(ch for ch in value if not unicodedata.combining(ch))
    value = value.upper()
    value = re.sub(r"[^A-Z0-9]+", "", value)
    return value


def detect_bank(text: str, beneficiary: Optional[str]) -> Optional[str]:
    material = f"{text}\n{beneficiary or ''}"
    compact = normalize_text_for_match(material)
    if "DIAMOND" in compact:
        return "DIAMOND"
    if any(token in compact for token in ("CLEEND", "CLEND", "CUEEND", "GLEEND")):
        return "CLEEND"
    if "AMD" in compact:
        return "AMD"
    return None


def normalize_date_for_excel(value: Optional[str]) -> Optional[str]:
    if not value:
        return None
    v = value.strip()
    for fmt in ("%d/%m/%Y", "%Y-%m-%d", "%d-%m-%Y", "%d/%m/%y"):
        try:
            dt = datetime.strptime(v, fmt)
            return dt.strftime("%d/%m/%Y")
        except Exception:
            continue
    compact = re.sub(r"[^A-Za-z0-9]", "", v).upper()
    m = re.fullmatch(r"(\d{1,2})([A-Z]{3})(\d{4})", compact)
    if m:
        token = m.group(2)
        month = MONTH_TOKEN_MAP.get(token)
        if month is not None:
            try:
                dt = datetime(int(m.group(3)), int(month), int(m.group(1)))
                return dt.strftime("%d/%m/%Y")
            except Exception:
                pass
    return value


def extract_first_date_value(text: str) -> Optional[str]:
    for pat in DATE_PATTERNS:
        m = pat.search(text)
        if m:
            return normalize_date_for_excel(m.group(1))

    m = ALPHA_MONTH_DATE_PATTERN.search(text)
    if m:
        return normalize_date_for_excel("".join(m.groups()))
    return None


def normalize_currency_code(value: str) -> Optional[str]:
    cur = (value or "").strip().upper()
    if cur in {"R$", "RS", "R"}:
        return "BRL"
    if cur == "US$":
        return "USD"
    return cur or None


def normalize_time_for_excel(value: Optional[str]) -> Optional[str]:
    if not value:
        return None
    parts = value.strip().split(":")
    if len(parts) < 2:
        return None
    try:
        h = int(parts[0])
        m = int(parts[1])
    except Exception:
        return None
    if h < 0 or h > 23 or m < 0 or m > 59:
        return None
    return f"{h:02d}:{m:02d}"


def _count_date_matches(text: str) -> int:
    total = 0
    for pat in DATE_PATTERNS:
        total += len(pat.findall(text))
    for day, month_token, year in ALPHA_MONTH_DATE_PATTERN.findall(text):
        if normalize_date_for_excel(f"{day}{month_token}{year}"):
            total += 1
    return total


def looks_like_single_receipt(text: str) -> tuple[bool, str]:
    low = text.lower()
    compact_low = re.sub(r"\s+", "", low)
    date_count = _count_date_matches(text)
    time_count = len(TIME_PATTERN.findall(text))
    amount_count = len(AMOUNT_FALLBACK_PATTERN.findall(text)) + len(AMOUNT_CURRENCY_PATTERN.findall(text))

    has_strong_kw = any(
        kw in low
        for kw in (
            "comprovante",
            "pix",
            "transferência",
            "transferencia",
            "pagamento",
            "recibo",
            "receipt",
            "收款",
            "转账",
            "付款",
            "交易",
        )
    )

    has_table_header = (
        ("data" in low and "hora" in low and "banco" in low and "transfer" in low)
        or ("horario" in low and "banco" in low and "transfer" in low)
    )
    has_balance_summary = (
        ("saldo antigo" in low or "saldoantigo" in compact_low)
        and ("saldo atual" in low or "saldoatual" in compact_low)
        and any(
            token in low or token.replace(" ", "") in compact_low
            for token in ("cheque", "dinheiro", "chq devolvido", "tx cheque", "no caiu")
        )
    )

    if has_balance_summary:
        return (False, "BALANCE_SUMMARY")
    if has_table_header and ("total" in low or not has_strong_kw):
        return (False, "TABULAR_TRANSFER_LIST")
    if has_table_header and (date_count >= 3 or time_count >= 3):
        return (False, "TABULAR_TRANSFER_LIST")
    if date_count >= 4 and amount_count >= 6:
        return (False, "MULTI_TRANSACTION_LIST")
    if not has_strong_kw and date_count >= 2 and amount_count >= 4:
        return (False, "WEAK_RECEIPT_SIGNAL")

    return (True, "OK")


def normalize_amount(value: str) -> Optional[float]:
    s = re.sub(r"[^\d,\.]", "", value.strip())
    if not s:
        return None
    if "," in s and "." in s:
        if s.rfind(",") > s.rfind("."):
            s = s.replace(".", "").replace(",", ".")
        else:
            s = s.replace(",", "")
    elif "," in s:
        if re.search(r",\d{1,2}$", s):
            s = s.replace(".", "").replace(",", ".")
        else:
            s = s.replace(",", "")
    try:
        return round(float(s), 2)
    except Exception:
        return None


def prepare_image_for_ocr(img: Image.Image, source_kind: str) -> Image.Image:
    out = img.convert("RGB")
    w, h = out.size
    is_thumb_like = source_kind == "msgattach_thumb_dat" or max(w, h) <= 420
    if not is_thumb_like:
        return out

    # Miniatures are tiny and blurry; upscale + contrast helps OCR.
    if max(w, h) <= 260:
        scale = 4
    elif max(w, h) <= 420:
        scale = 3
    else:
        scale = 2

    out = out.resize((w * scale, h * scale), LANCZOS_FILTER)
    gray = out.convert("L")
    gray = ImageOps.autocontrast(gray, cutoff=2)
    gray = gray.filter(ImageFilter.MedianFilter(size=3))
    gray = gray.filter(ImageFilter.SHARPEN)
    return gray.convert("RGB")


def parse_receipt_fields(text: str, ocr_conf: float, q_score: float) -> dict[str, Any]:
    lines = [ln.strip() for ln in text.splitlines() if ln.strip()]
    raw = "\n".join(lines)

    txn_date = extract_first_date_value(raw)

    txn_time: Optional[str] = None
    mt = TIME_PATTERN.search(raw)
    if mt:
        txn_time = mt.group(1)
    txn_time = normalize_time_for_excel(txn_time)

    currency: Optional[str] = None
    amount: Optional[float] = None
    amount_candidates: list[float] = []
    for m in AMOUNT_CURRENCY_PATTERN.finditer(raw):
        cur = m.group(1)
        val = normalize_amount(m.group(2))
        if val is not None:
            amount_candidates.append(val)
            currency = normalize_currency_code(cur)
    if not amount_candidates:
        for m in AMOUNT_FALLBACK_PATTERN.finditer(raw):
            val = normalize_amount(m.group(1))
            if val is not None:
                amount_candidates.append(val)
    if amount_candidates:
        amount = max(amount_candidates)
        if currency is None:
            currency = "BRL"

    beneficiary: Optional[str] = None
    low_lines = [ln.lower() for ln in lines]
    for idx, low in enumerate(low_lines):
        if any(k in low for k in BENEFICIARY_KEYS):
            original = lines[idx]
            if ":" in original:
                right = original.split(":", 1)[1].strip()
                if right:
                    beneficiary = right
                    break
            if idx + 1 < len(lines):
                nxt = lines[idx + 1].strip()
                if nxt:
                    beneficiary = nxt
                    break

    bank = detect_bank(raw, beneficiary)

    has_receipt_keyword = any(
        kw in raw.lower()
        for kw in [
            "pix",
            "comprovante",
            "transfer",
            "pagamento",
            "valor",
            "favorecido",
            "destinat",
            "recibo",
            "收款",
            "转账",
            "付款",
            "金额",
        ]
    )

    parse_conf = 0.0
    parse_conf += min(0.20, max(0.0, ocr_conf) * 0.20)
    parse_conf += 0.35 if amount is not None else 0.0
    parse_conf += 0.20 if txn_date else 0.0
    parse_conf += 0.10 if txn_time else 0.0
    parse_conf += 0.15 if beneficiary else 0.0
    parse_conf += 0.10 if bank else 0.0
    parse_conf += 0.10 if has_receipt_keyword else 0.0
    parse_conf += min(0.10, q_score * 0.10)
    parse_conf = round(min(1.0, parse_conf), 4)

    return {
        "txn_date": txn_date,
        "txn_time": txn_time,
        "beneficiary": beneficiary,
        "bank": bank,
        "amount": amount,
        "currency": currency,
        "parse_conf": parse_conf,
        "has_receipt_keyword": has_receipt_keyword,
    }


@dataclass
class QueueItem:
    file_id: str
    path: str
    source_kind: str
    ext: str
    size: int
    mtime: float
    first_seen: float
    attempts: int


@dataclass
class WeChatMessageRef:
    msg_svr_id: Optional[str]
    talker: Optional[str]
    create_time: float
    image_rel_path: Optional[str]
    thumb_rel_path: Optional[str]
    image_abs_path: Optional[Path]
    thumb_abs_path: Optional[Path]

    def preferred_context_path(self) -> Optional[Path]:
        if self.image_abs_path is not None:
            return self.image_abs_path
        return self.thumb_abs_path

    def group_hash(self) -> Optional[str]:
        ctx = self.preferred_context_path()
        if ctx is None:
            return None
        return extract_group_id_from_path(ctx)


@dataclass
class MediaResolution:
    original_source_path: Path
    original_source_kind: str
    resolved_path: Path
    resolved_source_kind: str
    client_source_path: Path
    resolution_source: str
    verification_status: str
    msg_ref: Optional[WeChatMessageRef]
    using_thumb_fallback: bool = False


def extract_group_id_from_path(path: Path) -> Optional[str]:
    parts = path.parts
    for idx, part in enumerate(parts):
        if part.lower() == "msgattach" and idx + 1 < len(parts):
            return parts[idx + 1]
    return None


class ClientResolver:
    def __init__(self, map_path: Path) -> None:
        self.map_path = map_path
        self._mtime: float = -1.0
        self._map: dict[str, str] = {}
        self.reload_if_needed(force=True)

    def _normalize_keys(self, data: dict[str, Any]) -> dict[str, str]:
        out: dict[str, str] = {}
        for k, v in data.items():
            key = str(k).strip().lower()
            val = str(v).strip()
            if key and val:
                out[key] = val
        return out

    def reload_if_needed(self, force: bool = False) -> None:
        if not self.map_path.exists():
            if force or self._map:
                self._map = {}
                self._mtime = -1.0
            return
        mtime = self.map_path.stat().st_mtime
        if not force and mtime == self._mtime:
            return
        try:
            raw = self.map_path.read_text(encoding="utf-8")
            data = json.loads(raw)
            if isinstance(data, dict):
                self._map = self._normalize_keys(data)
            else:
                self._map = {}
            self._mtime = mtime
        except Exception:
            self._map = {}
            self._mtime = mtime

    def resolve(self, source_path: Path) -> Optional[str]:
        self.reload_if_needed()
        gid = extract_group_id_from_path(source_path)
        if not gid:
            return None
        key = gid.strip().lower()
        if key in self._map:
            return self._map[key]
        return None


class WeChatDBResolver:
    def __init__(self, watch_roots: list[Path], merge_path: Path, refresh_seconds: int = 10) -> None:
        self.watch_roots = [p.resolve() for p in watch_roots]
        self.wx_dirs = [p.parent.resolve() for p in self.watch_roots]
        self.wechat_root = self.wx_dirs[0].parent if self.wx_dirs else None
        self.merge_path = merge_path.resolve()
        self.refresh_seconds = max(5, int(refresh_seconds))
        self._pywxdump: Any = None
        self._decode_bytes_extra: Any = None
        self._wx_key: Optional[str] = None
        self._wx_dir: Optional[Path] = None
        self._last_refresh = 0.0
        self._last_error: Optional[str] = None
        self._lock = threading.Lock()
        self._load_dependencies()
        self._load_account_info(force=True)

    @property
    def available(self) -> bool:
        return self._pywxdump is not None and self._decode_bytes_extra is not None and self._wx_key is not None and self._wx_dir is not None

    @property
    def last_error(self) -> Optional[str]:
        return self._last_error

    @property
    def selected_wx_dir(self) -> Optional[Path]:
        return self._wx_dir

    def _load_dependencies(self) -> None:
        try:
            import pywxdump  # type: ignore
            from pywxdump.db.dbMSG import get_BytesExtra  # type: ignore

            self._pywxdump = pywxdump
            self._decode_bytes_extra = get_BytesExtra
        except Exception as exc:
            self._last_error = f"pywxdump_unavailable:{type(exc).__name__}:{exc}"

    def _load_account_info(self, force: bool = False) -> bool:
        if not force and self._wx_key and self._wx_dir is not None:
            return True
        if self._pywxdump is None:
            return False
        try:
            infos = self._pywxdump.get_wx_info()
        except Exception as exc:
            self._last_error = f"wx_info_failed:{type(exc).__name__}:{exc}"
            return False
        if not infos:
            self._last_error = "wx_info_empty"
            return False

        chosen: Optional[dict[str, Any]] = None
        normalized_dirs = {path_to_normalized_windows(p) for p in self.wx_dirs}
        for info in infos:
            wx_dir_raw = str(info.get("wx_dir") or "").strip()
            if not wx_dir_raw:
                continue
            if normalize_windows_text(wx_dir_raw) in normalized_dirs:
                chosen = info
                break
        if chosen is None:
            chosen = infos[0]

        key = str(chosen.get("key") or "").strip()
        wx_dir_raw = str(chosen.get("wx_dir") or "").strip()
        if not key or not wx_dir_raw:
            self._last_error = "wx_info_missing_key_or_dir"
            return False

        self._wx_key = key
        self._wx_dir = Path(wx_dir_raw)
        if self.wechat_root is None:
            self.wechat_root = self._wx_dir.parent
        self._last_error = None
        return True

    def refresh_if_due(self, force: bool = False) -> bool:
        with self._lock:
            now = time.time()
            if not force and self.merge_path.exists() and (now - self._last_refresh) < self.refresh_seconds:
                return True
            if not self._load_account_info(force=force):
                return False
            self.merge_path.parent.mkdir(parents=True, exist_ok=True)
            try:
                assert self._pywxdump is not None
                assert self._wx_key is not None
                assert self._wx_dir is not None
                code, ret = self._pywxdump.all_merge_real_time_db(
                    self._wx_key,
                    str(self._wx_dir),
                    str(self.merge_path),
                )
            except Exception as exc:
                self._last_error = f"merge_failed:{type(exc).__name__}:{exc}"
                return False
            if not code:
                self._last_error = f"merge_failed:{ret}"
                return False
            self._last_refresh = now
            self._last_error = None
            return True

    def _absolute_path_from_rel(self, rel_path: Optional[str]) -> Optional[Path]:
        if not rel_path or self.wechat_root is None:
            return None
        parts = [part for part in PureWindowsPath(rel_path).parts if part not in ("\\", "/")]
        if not parts:
            return None
        return self.wechat_root.joinpath(*parts)

    def _extract_media_paths(self, bytes_extra: Any) -> tuple[Optional[str], Optional[str]]:
        image_rel: Optional[str] = None
        thumb_rel: Optional[str] = None
        try:
            decoded = self._decode_bytes_extra(bytes_extra) if self._decode_bytes_extra else {}
        except Exception:
            decoded = {}

        items = decoded.get("3") if isinstance(decoded, dict) else None
        if isinstance(items, list):
            for item in items:
                if not isinstance(item, dict):
                    continue
                key = str(item.get("1") or "").strip()
                value = str(item.get("2") or "").strip()
                if "filestorage" not in value.lower():
                    continue
                if key == "4" and image_rel is None:
                    image_rel = value
                elif key == "3" and thumb_rel is None:
                    thumb_rel = value

        if image_rel or thumb_rel:
            return image_rel, thumb_rel

        raw_text = str(decoded)
        matches = re.findall(r"(wxid_[^\\']+\\FileStorage\\[^']+)", raw_text)
        for match in matches:
            lowered = match.lower()
            if "\\image\\" in lowered and image_rel is None:
                image_rel = match
            elif "\\thumb\\" in lowered and thumb_rel is None:
                thumb_rel = match
        return image_rel, thumb_rel

    def _recent_messages(
        self,
        pivot_ts: float,
        lookback_sec: int,
        lookahead_sec: int,
        limit: int = 80,
    ) -> list[WeChatMessageRef]:
        if not self.refresh_if_due():
            return []
        if not self.merge_path.exists():
            return []

        lower = max(0, int(pivot_ts) - max(5, int(lookback_sec)))
        upper = int(pivot_ts) + max(1, int(lookahead_sec))
        conn = sqlite3.connect(str(self.merge_path))
        conn.row_factory = sqlite3.Row
        try:
            rows = conn.execute(
                """
                SELECT MsgSvrID, StrTalker, CreateTime, BytesExtra
                FROM MSG
                WHERE Type=3
                  AND CreateTime BETWEEN ? AND ?
                ORDER BY ABS(CreateTime - ?) ASC, CreateTime DESC
                LIMIT ?
                """,
                (lower, upper, int(pivot_ts), int(max(1, limit))),
            ).fetchall()
        finally:
            conn.close()

        out: list[WeChatMessageRef] = []
        for row in rows:
            image_rel, thumb_rel = self._extract_media_paths(row["BytesExtra"])
            if not image_rel and not thumb_rel:
                continue
            out.append(
                WeChatMessageRef(
                    msg_svr_id=str(row["MsgSvrID"]) if row["MsgSvrID"] is not None else None,
                    talker=str(row["StrTalker"]) if row["StrTalker"] is not None else None,
                    create_time=float(row["CreateTime"]),
                    image_rel_path=image_rel,
                    thumb_rel_path=thumb_rel,
                    image_abs_path=self._absolute_path_from_rel(image_rel),
                    thumb_abs_path=self._absolute_path_from_rel(thumb_rel),
                )
            )
        return out

    def _candidate_norms(self, path: Path) -> set[str]:
        norms = {path_to_normalized_windows(path)}
        if self.wechat_root is not None:
            try:
                rel = path.resolve().relative_to(self.wechat_root.resolve())
                norms.add(normalize_windows_text(str(PureWindowsPath(*rel.parts))))
            except Exception:
                pass
        return norms

    def find_message_for_path(self, path: Path, pivot_ts: float) -> Optional[WeChatMessageRef]:
        path_norms = self._candidate_norms(path)
        for msg in self._recent_messages(pivot_ts, lookback_sec=180, lookahead_sec=45, limit=120):
            msg_paths = {normalize_windows_text(p) for p in (msg.image_rel_path, msg.thumb_rel_path) if p}
            if msg.image_abs_path is not None:
                msg_paths.add(path_to_normalized_windows(msg.image_abs_path))
            if msg.thumb_abs_path is not None:
                msg_paths.add(path_to_normalized_windows(msg.thumb_abs_path))
            if path_norms & msg_paths:
                return msg
        return None

    def find_unique_message_for_group(self, group_hash: str, pivot_ts: float, window_sec: int) -> Optional[WeChatMessageRef]:
        target = str(group_hash or "").strip().lower()
        if not target:
            return None
        matches = [
            msg
            for msg in self._recent_messages(pivot_ts, lookback_sec=window_sec, lookahead_sec=5, limit=80)
            if (msg.group_hash() or "").strip().lower() == target
        ]
        if not matches:
            return None
        unique: dict[str, WeChatMessageRef] = {}
        for msg in matches:
            key = msg.msg_svr_id or f"{msg.talker}|{msg.create_time}|{msg.thumb_rel_path}|{msg.image_rel_path}"
            unique.setdefault(key, msg)
        if len(unique) != 1:
            return None
        return next(iter(unique.values()))

    def resolve_talker_display_name(self, talker: str) -> Optional[str]:
        talker = str(talker or "").strip()
        if not talker:
            return None
        if not self.refresh_if_due():
            return talker
        if not self.merge_path.exists():
            return talker

        conn = sqlite3.connect(str(self.merge_path))
        conn.row_factory = sqlite3.Row
        try:
            row = conn.execute(
                """
                SELECT Remark, NickName, Alias
                FROM Contact
                WHERE UserName=?
                LIMIT 1
                """,
                (talker,),
            ).fetchone()
        finally:
            conn.close()

        if row is None:
            return talker
        for key in ("Remark", "NickName", "Alias"):
            value = str(row[key] or "").strip()
            if value:
                return value
        return talker


class StateDB:
    def __init__(self, db_path: Path) -> None:
        self.db_path = db_path
        self.db_path.parent.mkdir(parents=True, exist_ok=True)
        self._conn = sqlite3.connect(str(db_path), check_same_thread=False)
        self._conn.row_factory = sqlite3.Row
        self._lock = threading.Lock()
        self._init_schema()

    def _init_schema(self) -> None:
        with self._lock:
            cur = self._conn.cursor()
            cur.executescript(
                """
                PRAGMA journal_mode=WAL;
                PRAGMA synchronous=NORMAL;

                CREATE TABLE IF NOT EXISTS files (
                    file_id TEXT PRIMARY KEY,
                    path TEXT NOT NULL,
                    source_kind TEXT NOT NULL,
                    ext TEXT NOT NULL,
                    size INTEGER NOT NULL,
                    mtime REAL NOT NULL,
                    ctime REAL NOT NULL,
                    status TEXT NOT NULL,
                    attempts INTEGER NOT NULL DEFAULT 0,
                    next_attempt REAL NOT NULL DEFAULT 0,
                    first_seen REAL NOT NULL,
                    last_seen REAL NOT NULL,
                    processed_at REAL,
                    sha256 TEXT,
                    last_error TEXT
                );
                CREATE INDEX IF NOT EXISTS idx_files_status_next ON files(status, next_attempt);
                CREATE INDEX IF NOT EXISTS idx_files_path ON files(path);

                CREATE TABLE IF NOT EXISTS receipts (
                    file_id TEXT PRIMARY KEY,
                    source_path TEXT NOT NULL,
                    source_kind TEXT NOT NULL,
                    ingested_at REAL NOT NULL,
                    sha256 TEXT NOT NULL,
                    txn_date TEXT,
                    txn_time TEXT,
                    client TEXT,
                    bank TEXT,
                    beneficiary TEXT,
                    amount REAL,
                    currency TEXT,
                    parse_conf REAL NOT NULL,
                    quality_score REAL NOT NULL,
                    ocr_engine TEXT NOT NULL,
                    ocr_conf REAL NOT NULL,
                    ocr_chars INTEGER NOT NULL,
                    review_needed INTEGER NOT NULL,
                    ocr_text TEXT,
                    parser_json TEXT,
                    msg_svr_id TEXT,
                    talker TEXT,
                    resolved_media_path TEXT,
                    resolution_source TEXT,
                    verification_status TEXT,
                    excel_sheet TEXT,
                    excel_row INTEGER
                );
                CREATE TABLE IF NOT EXISTS message_jobs (
                    msg_svr_id TEXT PRIMARY KEY,
                    talker TEXT NOT NULL,
                    talker_display TEXT,
                    thumb_path TEXT,
                    expected_image_path TEXT,
                    create_time REAL NOT NULL,
                    state TEXT NOT NULL,
                    first_seen_at REAL NOT NULL,
                    last_seen_at REAL NOT NULL,
                    ui_force_requested_at REAL,
                    ui_force_completed_at REAL,
                    ui_force_attempts INTEGER NOT NULL DEFAULT 0,
                    next_ui_attempt_at REAL NOT NULL DEFAULT 0,
                    last_ui_result TEXT,
                    batch_id TEXT
                );
                CREATE TABLE IF NOT EXISTS meta (
                    key TEXT PRIMARY KEY,
                    value TEXT NOT NULL,
                    updated_at REAL NOT NULL
                );
                """
            )
            self._ensure_column_exists(cur, "receipts", "client", "TEXT")
            self._ensure_column_exists(cur, "receipts", "bank", "TEXT")
            self._ensure_column_exists(cur, "receipts", "msg_svr_id", "TEXT")
            self._ensure_column_exists(cur, "receipts", "talker", "TEXT")
            self._ensure_column_exists(cur, "receipts", "resolved_media_path", "TEXT")
            self._ensure_column_exists(cur, "receipts", "resolution_source", "TEXT")
            self._ensure_column_exists(cur, "receipts", "verification_status", "TEXT")
            cur.execute("CREATE INDEX IF NOT EXISTS idx_receipts_sha256 ON receipts(sha256)")
            cur.execute("CREATE INDEX IF NOT EXISTS idx_receipts_msg_svr_id ON receipts(msg_svr_id)")
            cur.execute("CREATE INDEX IF NOT EXISTS idx_message_jobs_state_next ON message_jobs(state, next_ui_attempt_at, first_seen_at)")
            cur.execute("CREATE INDEX IF NOT EXISTS idx_message_jobs_talker_state ON message_jobs(talker, state, create_time DESC)")
            cur.execute("CREATE INDEX IF NOT EXISTS idx_message_jobs_expected_path ON message_jobs(expected_image_path)")
            self._conn.commit()

    @staticmethod
    def _ensure_column_exists(cur: sqlite3.Cursor, table_name: str, column_name: str, column_type: str) -> None:
        existing = {str(row[1]).lower() for row in cur.execute(f"PRAGMA table_info({table_name})").fetchall()}
        if column_name.lower() not in existing:
            cur.execute(f"ALTER TABLE {table_name} ADD COLUMN {column_name} {column_type}")

    @staticmethod
    def compute_file_id(path: Path, stat: os.stat_result) -> str:
        payload = f"{str(path).lower()}|{stat.st_size}|{stat.st_mtime_ns}"
        return hashlib.sha1(payload.encode("utf-8")).hexdigest()

    def upsert_candidate(self, path: Path, settle_seconds: int, source_event: str) -> Optional[str]:
        if not is_candidate(path):
            return None
        try:
            st = path.stat()
        except FileNotFoundError:
            return None
        if st.st_size <= 0:
            return None

        now = time.time()
        file_id = self.compute_file_id(path, st)
        source_kind = detect_source_kind(path)
        ext = path.suffix.lower()
        next_attempt = now + max(1, settle_seconds)

        with self._lock:
            cur = self._conn.cursor()
            existing = cur.execute(
                """
                SELECT file_id, status
                FROM files
                WHERE path=?
                ORDER BY last_seen DESC
                LIMIT 1
                """,
                (str(path),),
            ).fetchone()
            if existing is not None and existing["status"] in ("pending", "retry", "processing"):
                cur.execute(
                    """
                    UPDATE files
                    SET last_seen=?, mtime=?, size=?, next_attempt=MIN(next_attempt, ?)
                    WHERE file_id=?
                    """,
                    (
                        float(now),
                        float(st.st_mtime),
                        int(st.st_size),
                        float(next_attempt),
                        existing["file_id"],
                    ),
                )
                self._conn.commit()
                return str(existing["file_id"])

            cur.execute(
                """
                INSERT INTO files(file_id, path, source_kind, ext, size, mtime, ctime, status, attempts, next_attempt, first_seen, last_seen)
                VALUES(?, ?, ?, ?, ?, ?, ?, 'pending', 0, ?, ?, ?)
                ON CONFLICT(file_id) DO UPDATE SET
                    last_seen=excluded.last_seen,
                    mtime=excluded.mtime,
                    size=excluded.size,
                    next_attempt=CASE
                        WHEN files.status IN ('done', 'duplicate') THEN files.next_attempt
                        ELSE MIN(files.next_attempt, excluded.next_attempt)
                    END
                """,
                (
                    file_id,
                    str(path),
                    source_kind,
                    ext,
                    int(st.st_size),
                    float(st.st_mtime),
                    float(st.st_ctime),
                    float(next_attempt),
                    float(now),
                    float(now),
                ),
            )
            self._conn.commit()
        return file_id

    def get_meta(self, key: str) -> Optional[str]:
        with self._lock:
            row = self._conn.execute("SELECT value FROM meta WHERE key=? LIMIT 1", (key,)).fetchone()
            return None if row is None else str(row["value"])

    def get_meta_float(self, key: str) -> Optional[float]:
        raw = self.get_meta(key)
        if raw is None:
            return None
        try:
            return float(raw)
        except Exception:
            return None

    def set_meta(self, key: str, value: str) -> None:
        now = time.time()
        with self._lock:
            self._conn.execute(
                """
                INSERT INTO meta(key, value, updated_at)
                VALUES(?, ?, ?)
                ON CONFLICT(key) DO UPDATE SET
                    value=excluded.value,
                    updated_at=excluded.updated_at
                """,
                (key, value, float(now)),
            )
            self._conn.commit()

    def ignore_stale_queue(self, older_than_mtime: float) -> int:
        now = time.time()
        with self._lock:
            cur = self._conn.cursor()
            count = int(
                cur.execute(
                    """
                    SELECT COUNT(*)
                    FROM files
                    WHERE status IN ('pending', 'retry', 'processing')
                      AND mtime < ?
                    """,
                    (float(older_than_mtime),),
                ).fetchone()[0]
            )
            if count <= 0:
                return 0
            cur.execute(
                """
                UPDATE files
                SET status='ignored',
                    processed_at=?,
                    next_attempt=0,
                    last_error=CASE
                        WHEN last_error IS NULL OR last_error=''
                            THEN 'IGNORED_OLD_BACKLOG'
                        ELSE last_error
                    END
                WHERE status IN ('pending', 'retry', 'processing')
                  AND mtime < ?
                """,
                (float(now), float(older_than_mtime)),
            )
            self._conn.commit()
            return count

    def find_recent_msgattach_context_path(
        self,
        pivot_mtime: float,
        lookback_sec: int = 90,
        lookahead_sec: int = 15,
        limit: int = 24,
    ) -> Optional[str]:
        lower = float(pivot_mtime) - max(5, int(lookback_sec))
        upper = float(pivot_mtime) + max(1, int(lookahead_sec))
        with self._lock:
            rows = self._conn.execute(
                """
                SELECT path, mtime
                FROM files
                WHERE source_kind IN ('msgattach_image_dat', 'msgattach_thumb_dat', 'msgattach_image_plain')
                  AND mtime BETWEEN ? AND ?
                ORDER BY ABS(mtime - ?) ASC, mtime DESC
                LIMIT ?
                """,
                (lower, upper, float(pivot_mtime), int(max(1, limit))),
            ).fetchall()

        if not rows:
            return None

        unique_groups: dict[str, str] = {}
        for row in rows:
            candidate = str(row["path"])
            gid = extract_group_id_from_path(Path(candidate))
            if not gid:
                continue
            key = gid.strip().lower()
            unique_groups.setdefault(key, candidate)

        if len(unique_groups) == 1:
            return next(iter(unique_groups.values()))
        return None

    def get_latest_file_row_by_path(self, path: Path | str) -> Optional[sqlite3.Row]:
        with self._lock:
            row = self._conn.execute(
                """
                SELECT file_id, path, source_kind, status, attempts, first_seen, mtime, ctime, next_attempt, last_error
                FROM files
                WHERE path=?
                ORDER BY mtime DESC, last_seen DESC
                LIMIT 1
                """,
                (str(path),),
            ).fetchone()
            return row

    def find_recent_unresolved_msgattach_context_path(
        self,
        max_age_sec: int = 1800,
        limit: int = 40,
    ) -> Optional[str]:
        threshold = time.time() - max(60, int(max_age_sec))
        with self._lock:
            rows = self._conn.execute(
                """
                SELECT path, mtime, last_error
                FROM files
                WHERE source_kind IN ('msgattach_image_dat', 'msgattach_thumb_dat', 'msgattach_image_plain')
                  AND mtime >= ?
                  AND (
                    status IN ('retry', 'processing')
                    OR status='exception'
                    OR last_error IN ('WAITING_ORIGINAL_MEDIA', 'EXCEPTION_MISSING_CORE_FIELDS')
                  )
                ORDER BY mtime DESC
                LIMIT ?
                """,
                (float(threshold), int(max(1, limit))),
            ).fetchall()

        if not rows:
            return None

        unique_groups: dict[str, str] = {}
        for row in rows:
            candidate = str(row["path"])
            gid = extract_group_id_from_path(Path(candidate))
            if not gid:
                continue
            key = gid.strip().lower()
            unique_groups.setdefault(key, candidate)

        if len(unique_groups) == 1:
            return next(iter(unique_groups.values()))
        return None

    def claim_next(self) -> Optional[QueueItem]:
        now = time.time()
        with self._lock:
            cur = self._conn.cursor()
            cur.execute("BEGIN IMMEDIATE")
            row = cur.execute(
                """
                SELECT file_id, path, source_kind, ext, size, mtime, first_seen, attempts
                FROM files
                WHERE status IN ('pending', 'retry')
                  AND next_attempt <= ?
                ORDER BY mtime DESC, next_attempt ASC
                LIMIT 1
                """,
                (float(now),),
            ).fetchone()
            if row is None:
                self._conn.commit()
                return None
            next_attempt_count = int(row["attempts"]) + 1
            cur.execute(
                """
                UPDATE files
                SET status='processing', attempts=?, last_error=NULL
                WHERE file_id=?
                """,
                (next_attempt_count, row["file_id"]),
            )
            self._conn.commit()
            return QueueItem(
                file_id=row["file_id"],
                path=row["path"],
                source_kind=row["source_kind"],
                ext=row["ext"],
                size=int(row["size"]),
                mtime=float(row["mtime"]),
                first_seen=float(row["first_seen"]),
                attempts=next_attempt_count,
            )

    def mark_done(self, file_id: str, sha256: str, processed_at: float, note: Optional[str] = None) -> None:
        with self._lock:
            self._conn.execute(
                """
                UPDATE files
                SET status='done', processed_at=?, sha256=?, last_error=?
                WHERE file_id=?
                """,
                (processed_at, sha256, note[:1200] if note else None, file_id),
            )
            self._conn.commit()

    def resolve_related_file_paths(
        self,
        source_path: Path | str,
        exclude_file_id: str,
        sha256: str = "",
        reason: str = "RESOLVED_BY_LATER_SUCCESS",
    ) -> int:
        now = time.time()
        with self._lock:
            cur = self._conn.cursor()
            cur.execute(
                """
                UPDATE files
                SET status='done',
                    processed_at=?,
                    sha256=CASE WHEN sha256 IS NULL OR sha256='' THEN ? ELSE sha256 END,
                    last_error=?
                WHERE path=?
                  AND file_id<>?
                  AND status IN ('pending', 'retry', 'processing', 'exception')
                """,
                (float(now), sha256, reason[:1200], str(source_path), str(exclude_file_id)),
            )
            changed = int(cur.rowcount or 0)
            self._conn.commit()
            return changed

    def cleanup_stale_temp_orphans(self, max_age_sec: int = 600) -> int:
        threshold = time.time() - max(60, int(max_age_sec))
        now = time.time()
        with self._lock:
            rows = self._conn.execute(
                """
                SELECT file_id, path
                FROM files
                WHERE source_kind='temp_image'
                  AND status IN ('pending', 'retry', 'processing', 'exception')
                  AND last_seen <= ?
                ORDER BY last_seen ASC
                """,
                (float(threshold),),
            ).fetchall()

            stale_ids: list[str] = []
            for row in rows:
                if not Path(str(row["path"])).exists():
                    stale_ids.append(str(row["file_id"]))

            if not stale_ids:
                return 0

            self._conn.executemany(
                """
                UPDATE files
                SET status='done',
                    processed_at=?,
                    last_error='STALE_TEMP_ORPHAN'
                WHERE file_id=?
                """,
                [(float(now), file_id) for file_id in stale_ids],
            )
            self._conn.commit()
            return len(stale_ids)

    def mark_retry(
        self,
        file_id: str,
        attempts: int,
        retry_base_sec: int,
        err: str,
        max_retries: int,
        delay_override_sec: Optional[int] = None,
    ) -> None:
        if max_retries > 0 and attempts >= max_retries:
            status = "failed"
            next_attempt = 0.0
        else:
            status = "retry"
            if delay_override_sec is not None:
                backoff = max(3, int(delay_override_sec))
            else:
                backoff = min(retry_base_sec * (2 ** max(0, attempts - 1)), 3600)
            next_attempt = time.time() + float(backoff)

        with self._lock:
            self._conn.execute(
                """
                UPDATE files
                SET status=?, next_attempt=?, last_error=?
                WHERE file_id=?
                """,
                (status, next_attempt, err[:1200], file_id),
            )
            self._conn.commit()

    def mark_hold(self, file_id: str, reason: str, delay_sec: int = 120) -> None:
        next_attempt = time.time() + max(3, delay_sec)
        with self._lock:
            self._conn.execute(
                """
                UPDATE files
                SET status='retry', next_attempt=?, last_error=?
                WHERE file_id=?
                """,
                (next_attempt, reason[:1200], file_id),
            )
            self._conn.commit()

    def mark_exception(self, file_id: str, reason: str) -> None:
        with self._lock:
            self._conn.execute(
                """
                UPDATE files
                SET status='exception', next_attempt=0, processed_at=?, last_error=?
                WHERE file_id=?
                """,
                (float(time.time()), reason[:1200], file_id),
            )
            self._conn.commit()

    def receipt_exists(self, file_id: str) -> bool:
        with self._lock:
            row = self._conn.execute("SELECT 1 FROM receipts WHERE file_id=? LIMIT 1", (file_id,)).fetchone()
            return row is not None

    def receipt_msg_exists(self, msg_svr_id: Optional[str]) -> bool:
        if not msg_svr_id:
            return False
        with self._lock:
            row = self._conn.execute("SELECT 1 FROM receipts WHERE msg_svr_id=? LIMIT 1", (str(msg_svr_id),)).fetchone()
            return row is not None

    def requeue_mapped_missing_client(
        self,
        resolver: "ClientResolver",
        max_age_hours: int = 3,
        limit: int = 1200,
    ) -> int:
        """
        Requeue files that were previously marked with MISSING_CLIENT_MAP but now
        have a valid client mapping.
        """
        threshold = time.time() - max(1, int(max_age_hours)) * 3600
        now = time.time()
        with self._lock:
            cur = self._conn.cursor()
            rows = cur.execute(
                """
                SELECT file_id, path
                FROM files
                WHERE status='done'
                  AND last_error LIKE 'MISSING_CLIENT_MAP:%'
                  AND mtime >= ?
                ORDER BY mtime DESC
                LIMIT ?
                """,
                (float(threshold), int(max(1, limit))),
            ).fetchall()

            to_requeue: list[str] = []
            for row in rows:
                p = Path(row["path"])
                if resolver.resolve(p):
                    to_requeue.append(str(row["file_id"]))

            if not to_requeue:
                return 0

            cur.executemany(
                """
                UPDATE files
                SET status='retry', attempts=0, next_attempt=?, processed_at=NULL, last_error=NULL
                WHERE file_id=?
                """,
                [(float(now), fid) for fid in to_requeue],
            )
            self._conn.commit()
            return len(to_requeue)

    def backfill_receipt_context(self, resolver: "ClientResolver", limit: int = 5000) -> int:
        with self._lock:
            cur = self._conn.cursor()
            rows = cur.execute(
                """
                SELECT file_id, source_path, client, bank, beneficiary, ocr_text
                FROM receipts
                WHERE client IS NULL OR bank IS NULL
                LIMIT ?
                """,
                (int(max(1, limit)),),
            ).fetchall()

            updates: list[tuple[Optional[str], Optional[str], str]] = []
            for row in rows:
                source_path = Path(str(row["source_path"]))
                client = row["client"] if row["client"] else resolver.resolve(source_path)
                bank = row["bank"]
                if not bank:
                    bank = detect_bank(f"{row['ocr_text'] or ''}\n{client or ''}", row["beneficiary"])
                if client != row["client"] or bank != row["bank"]:
                    updates.append((client, bank, str(row["file_id"])))

            if not updates:
                return 0

            cur.executemany(
                """
                UPDATE receipts
                SET client=?, bank=?
                WHERE file_id=?
                """,
                updates,
            )
            self._conn.commit()
            return len(updates)

    def receipt_sha_exists(self, sha256: str) -> bool:
        if not sha256:
            return False
        with self._lock:
            row = self._conn.execute("SELECT 1 FROM receipts WHERE sha256=? LIMIT 1", (sha256,)).fetchone()
            return row is not None

    def insert_receipt(self, payload: dict[str, Any]) -> None:
        with self._lock:
            self._conn.execute(
                """
                INSERT OR REPLACE INTO receipts(
                    file_id, source_path, source_kind, ingested_at, sha256,
                    txn_date, txn_time, client, bank, beneficiary, amount, currency,
                    parse_conf, quality_score, ocr_engine, ocr_conf, ocr_chars,
                    review_needed, ocr_text, parser_json, msg_svr_id, talker,
                    resolved_media_path, resolution_source, verification_status,
                    excel_sheet, excel_row
                )
                VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)
                """,
                (
                    payload["file_id"],
                    payload["source_path"],
                    payload["source_kind"],
                    payload["ingested_at"],
                    payload["sha256"],
                    payload.get("txn_date"),
                    payload.get("txn_time"),
                    payload.get("client"),
                    payload.get("bank"),
                    payload.get("beneficiary"),
                    payload.get("amount"),
                    payload.get("currency"),
                    payload["parse_conf"],
                    payload["quality_score"],
                    payload["ocr_engine"],
                    payload["ocr_conf"],
                    payload["ocr_chars"],
                    1 if payload["review_needed"] else 0,
                    payload.get("ocr_text"),
                    payload.get("parser_json"),
                    payload.get("msg_svr_id"),
                    payload.get("talker"),
                    payload.get("resolved_media_path"),
                    payload.get("resolution_source"),
                    payload.get("verification_status"),
                    payload.get("excel_sheet"),
                    payload.get("excel_row"),
                ),
            )
            self._conn.commit()

    def ensure_message_job(
        self,
        msg_svr_id: Optional[str],
        talker: Optional[str],
        talker_display: Optional[str],
        thumb_path: Optional[Path],
        expected_image_path: Optional[Path],
        create_time: float,
        first_seen_at: float,
    ) -> None:
        msg_svr_id = str(msg_svr_id or "").strip()
        talker = str(talker or "").strip()
        if not msg_svr_id or not talker:
            return

        now = time.time()
        expected_str = str(expected_image_path) if expected_image_path else None
        thumb_str = str(thumb_path) if thumb_path else None
        display_str = str(talker_display or "").strip() or None
        first_seen = float(first_seen_at or now)
        with self._lock:
            row = self._conn.execute(
                """
                SELECT first_seen_at
                FROM message_jobs
                WHERE msg_svr_id=?
                LIMIT 1
                """,
                (msg_svr_id,),
            ).fetchone()
            preserved_first_seen = float(row["first_seen_at"]) if row is not None else first_seen
            self._conn.execute(
                """
                INSERT INTO message_jobs(
                    msg_svr_id, talker, talker_display, thumb_path, expected_image_path,
                    create_time, state, first_seen_at, last_seen_at, next_ui_attempt_at
                )
                VALUES(?, ?, ?, ?, ?, ?, 'NEW', ?, ?, 0)
                ON CONFLICT(msg_svr_id) DO UPDATE SET
                    talker=excluded.talker,
                    talker_display=COALESCE(NULLIF(excluded.talker_display, ''), message_jobs.talker_display),
                    thumb_path=COALESCE(NULLIF(excluded.thumb_path, ''), message_jobs.thumb_path),
                    expected_image_path=COALESCE(NULLIF(excluded.expected_image_path, ''), message_jobs.expected_image_path),
                    create_time=CASE
                        WHEN excluded.create_time > 0 THEN excluded.create_time
                        ELSE message_jobs.create_time
                    END,
                    last_seen_at=excluded.last_seen_at
                """,
                (
                    msg_svr_id,
                    talker,
                    display_str,
                    thumb_str,
                    expected_str,
                    float(create_time),
                    preserved_first_seen,
                    float(now),
                ),
            )
            self._conn.commit()

    def get_message_job(self, msg_svr_id: Optional[str]) -> Optional[sqlite3.Row]:
        msg_svr_id = str(msg_svr_id or "").strip()
        if not msg_svr_id:
            return None
        with self._lock:
            return self._conn.execute(
                """
                SELECT *
                FROM message_jobs
                WHERE msg_svr_id=?
                LIMIT 1
                """,
                (msg_svr_id,),
            ).fetchone()

    def get_message_job_by_expected_path(self, expected_image_path: Path | str) -> Optional[sqlite3.Row]:
        with self._lock:
            return self._conn.execute(
                """
                SELECT *
                FROM message_jobs
                WHERE expected_image_path=?
                ORDER BY last_seen_at DESC
                LIMIT 1
                """,
                (str(expected_image_path),),
            ).fetchone()

    def find_prior_pending_message_job(
        self,
        talker: Optional[str],
        create_time: float,
        msg_svr_id: Optional[str],
    ) -> Optional[sqlite3.Row]:
        talker_value = str(talker or "").strip()
        msg_value = str(msg_svr_id or "").strip()
        if not talker_value or create_time <= 0 or not msg_value:
            return None
        with self._lock:
            return self._conn.execute(
                """
                SELECT msg_svr_id, create_time, state, expected_image_path, thumb_path
                FROM message_jobs
                WHERE talker=?
                  AND msg_svr_id<>?
                  AND state NOT IN ('RESOLVED', 'THUMB_FALLBACK', 'EXCEPTION')
                  AND (
                    create_time < ?
                    OR (create_time = ? AND msg_svr_id < ?)
                  )
                ORDER BY create_time ASC, msg_svr_id ASC
                LIMIT 1
                """,
                (talker_value, msg_value, float(create_time), float(create_time), msg_value),
            ).fetchone()

    def set_message_job_state(
        self,
        msg_svr_id: Optional[str],
        state: str,
        note: Optional[str] = None,
        next_ui_attempt_at: Optional[float] = None,
        batch_id: Optional[str] = None,
        reset_batch: bool = False,
        touch_ui_requested: bool = False,
        touch_ui_completed: bool = False,
    ) -> None:
        msg_svr_id = str(msg_svr_id or "").strip()
        if not msg_svr_id:
            return

        now = time.time()
        with self._lock:
            row = self._conn.execute(
                """
                SELECT state, next_ui_attempt_at, batch_id, last_ui_result
                FROM message_jobs
                WHERE msg_svr_id=?
                LIMIT 1
                """,
                (msg_svr_id,),
            ).fetchone()
            if row is None:
                return

            current_state = str(row["state"] or "")
            current_note = str(row["last_ui_result"] or "").strip()
            effective_state = state
            if current_state == "RESOLVED" and state != "RESOLVED":
                effective_state = "RESOLVED"
            elif current_state == "EXCEPTION" and state not in ("EXCEPTION", "RESOLVED"):
                effective_state = "EXCEPTION"
            elif current_state == "THUMB_FALLBACK" and state != "EXCEPTION":
                effective_state = "THUMB_FALLBACK"

            effective_next = float(next_ui_attempt_at) if next_ui_attempt_at is not None else float(row["next_ui_attempt_at"] or 0.0)
            effective_batch = None if reset_batch else (batch_id if batch_id is not None else row["batch_id"])
            if note:
                note_value = note[:1200]
                if current_note.startswith("ui_") and not note_value.startswith("ui_") and effective_state in {"RESOLVED", "THUMB_FALLBACK", "EXCEPTION"}:
                    effective_note = current_note
                else:
                    effective_note = note_value
            else:
                effective_note = current_note

            self._conn.execute(
                """
                UPDATE message_jobs
                SET state=?,
                    last_seen_at=?,
                    next_ui_attempt_at=?,
                    batch_id=?,
                    last_ui_result=?,
                    ui_force_requested_at=CASE WHEN ? THEN ? ELSE ui_force_requested_at END,
                    ui_force_completed_at=CASE WHEN ? THEN ? ELSE ui_force_completed_at END
                WHERE msg_svr_id=?
                """,
                (
                    effective_state,
                    float(now),
                    effective_next,
                    effective_batch,
                    effective_note,
                    1 if touch_ui_requested else 0,
                    float(now),
                    1 if touch_ui_completed else 0,
                    float(now),
                    msg_svr_id,
                ),
            )
            self._conn.commit()

    def mark_message_job_resolved(self, msg_svr_id: Optional[str], note: Optional[str] = None) -> None:
        self.set_message_job_state(
            msg_svr_id,
            state="RESOLVED",
            note=note,
            next_ui_attempt_at=0.0,
            reset_batch=True,
            touch_ui_completed=True,
        )

    def mark_message_job_thumb_fallback(self, msg_svr_id: Optional[str], note: Optional[str] = None) -> None:
        self.set_message_job_state(
            msg_svr_id,
            state="THUMB_FALLBACK",
            note=note,
            next_ui_attempt_at=0.0,
            reset_batch=True,
        )

    def mark_message_job_exception(self, msg_svr_id: Optional[str], note: Optional[str] = None) -> None:
        self.set_message_job_state(
            msg_svr_id,
            state="EXCEPTION",
            note=note,
            next_ui_attempt_at=0.0,
            reset_batch=True,
            touch_ui_completed=True,
        )

    def message_job_is_terminal(self, msg_svr_id: Optional[str]) -> bool:
        row = self.get_message_job(msg_svr_id)
        if row is None:
            return False
        return str(row["state"] or "") in {"RESOLVED", "THUMB_FALLBACK", "EXCEPTION"}

    def resolve_message_job_paths(self, msg_svr_id: Optional[str], exclude_file_id: str, sha256: str = "") -> int:
        row = self.get_message_job(msg_svr_id)
        if row is None:
            return 0

        paths = [str(row["thumb_path"] or "").strip(), str(row["expected_image_path"] or "").strip()]
        paths = [path for path in paths if path]
        if not paths:
            return 0

        now = time.time()
        placeholders = ",".join("?" for _ in paths)
        params: list[Any] = [float(now), sha256, "RESOLVED_BY_LATER_SUCCESS"]
        params.extend(paths)
        params.extend([str(exclude_file_id)])
        with self._lock:
            cur = self._conn.cursor()
            cur.execute(
                f"""
                UPDATE files
                SET status='done',
                    processed_at=?,
                    sha256=CASE WHEN sha256 IS NULL OR sha256='' THEN ? ELSE sha256 END,
                    last_error=?
                WHERE path IN ({placeholders})
                  AND file_id<>?
                  AND status IN ('pending', 'retry', 'processing', 'exception')
                """,
                params,
            )
            changed = int(cur.rowcount or 0)
            self._conn.commit()
            return changed

    def set_msg_cursor(self, create_time: float, msg_svr_id: Optional[str]) -> None:
        if create_time <= 0:
            return
        self.set_meta("last_msg_cursor", f"{int(create_time)}|{str(msg_svr_id or '').strip()}")

    def claim_ui_batch(self) -> tuple[Optional[str], list[dict[str, Any]]]:
        now = time.time()
        with self._lock:
            cur = self._conn.cursor()
            cur.execute("BEGIN IMMEDIATE")
            talker_row = cur.execute(
                """
                SELECT talker
                FROM message_jobs
                WHERE state='UI_FORCE_PENDING'
                  AND next_ui_attempt_at <= ?
                  AND talker_display IS NOT NULL
                  AND expected_image_path IS NOT NULL
                  AND thumb_path IS NOT NULL
                ORDER BY first_seen_at ASC, create_time DESC
                LIMIT 1
                """,
                (float(now),),
            ).fetchone()
            if talker_row is None:
                self._conn.commit()
                return None, []

            talker = str(talker_row["talker"])
            rows = cur.execute(
                """
                SELECT msg_svr_id, talker, talker_display, expected_image_path, thumb_path, create_time, ui_force_attempts
                FROM message_jobs
                WHERE talker=?
                  AND state='UI_FORCE_PENDING'
                  AND next_ui_attempt_at <= ?
                  AND talker_display IS NOT NULL
                  AND expected_image_path IS NOT NULL
                  AND thumb_path IS NOT NULL
                ORDER BY create_time DESC, msg_svr_id DESC
                """,
                (talker, float(now)),
            ).fetchall()
            if not rows:
                self._conn.commit()
                return None, []

            batch_id = hashlib.sha1(f"{talker}|{now}".encode("utf-8")).hexdigest()[:16]
            msg_ids = [str(row["msg_svr_id"]) for row in rows]
            cur.executemany(
                """
                UPDATE message_jobs
                SET state='UI_FORCE_RUNNING',
                    batch_id=?,
                    ui_force_requested_at=?,
                    ui_force_attempts=ui_force_attempts + 1,
                    last_seen_at=?
                WHERE msg_svr_id=?
                """,
                [(batch_id, float(now), float(now), msg_id) for msg_id in msg_ids],
            )
            self._conn.commit()

        jobs: list[dict[str, Any]] = []
        for row in rows:
            jobs.append(
                {
                    "msg_svr_id": str(row["msg_svr_id"]),
                    "talker": str(row["talker"]),
                    "talker_display": str(row["talker_display"]),
                    "expected_image_path": str(row["expected_image_path"]),
                    "thumb_path": str(row["thumb_path"]),
                    "create_time": float(row["create_time"]),
                    "ui_force_attempts": int(row["ui_force_attempts"]) + 1,
                }
            )
        return batch_id, jobs

    def finish_ui_batch(
        self,
        batch_id: Optional[str],
        resolved_msg_ids: list[str],
        note: str,
        backoff_seconds: list[int],
        resolved_notes_by_msg_id: Optional[dict[str, str]] = None,
    ) -> None:
        batch_id = str(batch_id or "").strip()
        if not batch_id:
            return

        now = time.time()
        resolved_set = {str(msg_id) for msg_id in resolved_msg_ids if str(msg_id).strip()}
        resolved_notes = {str(key): str(value)[:1200] for key, value in (resolved_notes_by_msg_id or {}).items() if str(key).strip() and str(value).strip()}
        with self._lock:
            rows = self._conn.execute(
                """
                SELECT msg_svr_id, ui_force_attempts
                FROM message_jobs
                WHERE batch_id=?
                """,
                (batch_id,),
            ).fetchall()
            cur = self._conn.cursor()
            for row in rows:
                msg_svr_id = str(row["msg_svr_id"])
                if msg_svr_id in resolved_set:
                    success_note = resolved_notes.get(msg_svr_id, note[:1200])
                    cur.execute(
                        """
                        UPDATE message_jobs
                        SET state='WAITING_ORIGINAL',
                            batch_id=NULL,
                            last_seen_at=?,
                            next_ui_attempt_at=0,
                            last_ui_result=?,
                            ui_force_completed_at=?
                        WHERE msg_svr_id=?
                        """,
                        (float(now), success_note, float(now), msg_svr_id),
                    )
                    continue

                attempts = int(row["ui_force_attempts"] or 0)
                idx = min(max(0, attempts - 1), max(0, len(backoff_seconds) - 1))
                delay = int(backoff_seconds[idx]) if backoff_seconds else 10
                cur.execute(
                    """
                    UPDATE message_jobs
                    SET state='UI_FORCE_PENDING',
                        batch_id=NULL,
                        last_seen_at=?,
                        next_ui_attempt_at=?,
                        last_ui_result=?
                    WHERE msg_svr_id=?
                    """,
                    (float(now), float(now + max(3, delay)), note[:1200], msg_svr_id),
                )
            self._conn.commit()

    def close(self) -> None:
        with self._lock:
            self._conn.close()


GOOGLE_SHEETS_SCOPES = ["https://www.googleapis.com/auth/spreadsheets"]


def normalize_header_text(value: Any) -> str:
    return "".join(ch for ch in unicodedata.normalize("NFKD", str(value or "")) if not unicodedata.combining(ch)).strip().upper()


def normalize_header_cells(values: list[Any], expected_headers: list[str]) -> list[str]:
    out: list[str] = []
    for value in values[: len(expected_headers)]:
        out.append(normalize_header_text(value))
    while len(out) < len(expected_headers):
        out.append("")
    return out


def extract_google_sheet_ref(value: str) -> tuple[str, Optional[int]]:
    raw = (value or "").strip()
    if not raw:
        raise ValueError("Google Sheets destination is empty")

    if raw.startswith("http://") or raw.startswith("https://"):
        parsed = urlparse(raw)
        match = re.search(r"/spreadsheets/d/([a-zA-Z0-9\-_]+)", parsed.path)
        if not match:
            raise ValueError(f"Unable to extract spreadsheet id from URL: {raw}")
        gid_value = parse_qs(parsed.query).get("gid", [None])[0]
        gid = int(gid_value) if gid_value and str(gid_value).isdigit() else None
        return match.group(1), gid

    if re.fullmatch(r"[a-zA-Z0-9\-_]{20,}", raw):
        return raw, None

    raise ValueError(f"Invalid Google Sheets reference: {raw}")


class RowSink:
    def append(self, row_payload: dict[str, Any], review_needed: bool) -> tuple[str, int]:
        raise NotImplementedError


class ExcelSink(RowSink):
    def __init__(self, excel_path: Path, verification_column_name: str = DEFAULT_VERIFICATION_COLUMN_NAME) -> None:
        self.excel_path = excel_path
        self.excel_path.parent.mkdir(parents=True, exist_ok=True)
        self._lock = threading.Lock()
        self.headers = build_lanc_headers(verification_column_name)
        self.expected_header_normalized = [normalize_header_text(header) for header in self.headers]
        self._init_workbook()

    def _sheet_has_expected_header(self, wb: Any, sheet_name: str) -> bool:
        if sheet_name not in wb.sheetnames:
            return False
        ws = wb[sheet_name]
        first_row = [cell for cell in next(ws.iter_rows(min_row=1, max_row=1, values_only=True), tuple())]
        return normalize_header_cells(first_row, self.headers) == self.expected_header_normalized

    def _create_fresh_workbook(self) -> None:
        wb = Workbook()
        ws = wb.active
        ws.title = "Lancamentos"
        ws.append(self.headers)
        ws2 = wb.create_sheet("Revisar")
        ws2.append(self.headers)
        wb.save(self.excel_path)
        wb.close()

    def _init_workbook(self) -> None:
        if not self.excel_path.exists():
            self._create_fresh_workbook()
            return

        wb = load_workbook(self.excel_path)
        ok_layout = self._sheet_has_expected_header(wb, "Lancamentos") and self._sheet_has_expected_header(wb, "Revisar")
        wb.close()

        if ok_layout:
            return

        ts = datetime.now().strftime("%Y%m%d_%H%M%S")
        legacy = self.excel_path.with_name(f"{self.excel_path.stem}_legacy_{ts}{self.excel_path.suffix}")
        try:
            self.excel_path.replace(legacy)
        except Exception:
            pass
        self._create_fresh_workbook()

    def append(self, row_payload: dict[str, Any], review_needed: bool) -> tuple[str, int]:
        with self._lock:
            wb = load_workbook(self.excel_path)
            sheet = "Revisar" if review_needed else "Lancamentos"
            if sheet not in wb.sheetnames:
                ws = wb.create_sheet(sheet)
                ws.append(self.headers)
            ws = wb[sheet]
            ws.append(
                [
                    row_payload.get("client"),
                    row_payload.get("txn_date"),
                    row_payload.get("txn_time"),
                    row_payload.get("bank"),
                    row_payload.get("amount"),
                    row_payload.get("verification_status"),
                ]
            )
            row_idx = ws.max_row
            wb.save(self.excel_path)
            wb.close()
            return (sheet, row_idx)


class GoogleSheetsSink(RowSink):
    def __init__(
        self,
        spreadsheet_ref: str,
        credentials_path: Optional[Path],
        main_worksheet: Optional[str] = None,
        review_worksheet: Optional[str] = "Revisar",
        verification_column_name: str = DEFAULT_VERIFICATION_COLUMN_NAME,
    ) -> None:
        self.spreadsheet_id, self.preferred_gid = extract_google_sheet_ref(spreadsheet_ref)
        self.credentials_path = credentials_path
        self.main_worksheet = (main_worksheet or "").strip() or None
        self.review_worksheet = (review_worksheet or "").strip() or None
        self._lock = threading.Lock()
        self.auth_mode = "unknown"
        self.headers = build_lanc_headers(verification_column_name)
        self.expected_header_normalized = [normalize_header_text(header) for header in self.headers]
        self._client = self._build_client()
        self._spreadsheet = self._client.open_by_key(self.spreadsheet_id)
        self._worksheets_by_title: dict[str, Any] = {}
        self._sheet_order: list[str] = []
        self._headers_verified: set[str] = set()
        self._main_sheet_title: Optional[str] = None
        self.spreadsheet_title: Optional[str] = getattr(self._spreadsheet, "title", None)
        self._refresh_sheet_cache()
        self._main_sheet_title = self._resolve_main_sheet_title()
        self._ensure_header(self._main_sheet_title)
        if self.review_worksheet and self.review_worksheet != self._main_sheet_title:
            self._ensure_sheet_exists(self.review_worksheet)
            self._ensure_header(self.review_worksheet)

    def _build_client(self) -> Any:
        import gspread  # type: ignore

        client = None
        errors: list[str] = []

        if self.credentials_path:
            if self.credentials_path.exists():
                try:
                    client = gspread.service_account(filename=str(self.credentials_path))
                    self.auth_mode = f"service_account:{self.credentials_path.name}"
                except Exception as exc:
                    errors.append(f"service account credentials failed: {exc}")
            else:
                errors.append(f"credential file not found: {self.credentials_path}")

        if client is None:
            try:
                import google.auth  # type: ignore

                creds, _project = google.auth.default(scopes=GOOGLE_SHEETS_SCOPES)
                client = gspread.authorize(creds)
                self.auth_mode = "application_default_credentials"
            except Exception as exc:
                errors.append(f"application default credentials failed: {exc}")

        if client is None:
            joined = " | ".join(errors) if errors else "no credentials configured"
            raise RuntimeError(
                "Google Sheets auth is not configured. "
                "Add a service account JSON at the configured path or login with "
                "`gcloud auth application-default login`. "
                f"Details: {joined}"
            )

        return client

    def _refresh_sheet_cache(self) -> None:
        worksheets = self._spreadsheet.worksheets()
        self._worksheets_by_title = {ws.title: ws for ws in worksheets}
        self._sheet_order = [ws.title for ws in worksheets]

    def _resolve_main_sheet_title(self) -> str:
        if self.main_worksheet:
            return self._ensure_sheet_exists(self.main_worksheet)

        if self.preferred_gid is not None:
            for title, worksheet in self._worksheets_by_title.items():
                sheet_id = int(getattr(worksheet, "id", 0) or 0)
                if sheet_id == self.preferred_gid:
                    return title

        if not self._sheet_order:
            raise RuntimeError("Google spreadsheet has no worksheets")
        return self._sheet_order[0]

    def _ensure_sheet_exists(self, title: str) -> str:
        if title in self._worksheets_by_title:
            return title

        try:
            self._spreadsheet.add_worksheet(title=title, rows=1000, cols=max(6, len(self.headers)))
        except Exception:
            self._refresh_sheet_cache()
            if title in self._worksheets_by_title:
                return title
            raise

        self._refresh_sheet_cache()
        return title

    def _ensure_header(self, title: str) -> None:
        if title in self._headers_verified:
            return

        worksheet = self._worksheets_by_title[title]
        values = worksheet.row_values(1)
        if not values or not any(str(v).strip() for v in values):
            worksheet.update(range_name=sheet_header_range(self.headers), values=[self.headers])
        elif normalize_header_cells(values, self.headers) != self.expected_header_normalized:
            legacy_headers = BASE_LANC_HEADERS
            legacy_expected = [normalize_header_text(header) for header in legacy_headers]
            if normalize_header_cells(values, legacy_headers) == legacy_expected:
                worksheet.update(range_name=sheet_header_range(self.headers), values=[self.headers])
            else:
                print(f"[WARN] worksheet_header_unexpected | worksheet={title} | found={values}")
        self._headers_verified.add(title)

    def _target_sheet(self, review_needed: bool) -> str:
        del review_needed
        main_title = self._main_sheet_title or self._resolve_main_sheet_title()
        return main_title

    def append(self, row_payload: dict[str, Any], review_needed: bool) -> tuple[str, int]:
        with self._lock:
            title = self._target_sheet(review_needed)
            self._ensure_header(title)
            worksheet = self._worksheets_by_title[title]
            worksheet.append_row(
                [
                    row_payload.get("client"),
                    row_payload.get("txn_date"),
                    row_payload.get("txn_time"),
                    row_payload.get("bank"),
                    row_payload.get("amount"),
                    row_payload.get("verification_status"),
                ],
                value_input_option="USER_ENTERED",
                table_range=sheet_table_range(self.headers),
            )
            row_idx = len(worksheet.col_values(1))
            return title, row_idx


class IngestEventHandler(FileSystemEventHandler):  # type: ignore[misc]
    def __init__(self, db: StateDB, settle_seconds: int) -> None:
        self.db = db
        self.settle_seconds = settle_seconds

    def on_created(self, event: Any) -> None:
        if event.is_directory:
            return
        self.db.upsert_candidate(Path(event.src_path), self.settle_seconds, "created")

    def on_modified(self, event: Any) -> None:
        if event.is_directory:
            return
        self.db.upsert_candidate(Path(event.src_path), self.settle_seconds, "modified")


@dataclass
class Config:
    watch_roots: list[Path]
    db_path: Path
    db_merge_path: Path
    excel_path: Path
    sink_mode: str
    gsheet_ref: Optional[str]
    gsheet_worksheet: Optional[str]
    gsheet_review_worksheet: Optional[str]
    google_credentials_path: Optional[Path]
    verification_column_name: str
    client_map_path: Path
    resolution_mode: str
    settle_seconds: int
    reconcile_seconds: int
    recent_files_hours: int
    idle_sleep_seconds: float
    retry_base_seconds: int
    min_confidence: float
    max_retries: int
    original_wait_seconds: int
    temp_correlation_seconds: int
    ui_force_download_enabled: bool
    ui_force_delay_seconds: int
    ui_force_scope: str
    ui_focus_policy: str
    ui_batch_mode: str
    ui_item_timeout_seconds: int
    ui_retry_backoff_seconds: list[int]
    disable_watchdog: bool


def reconcile_scan(cfg: Config, db: StateDB) -> int:
    count = 0
    now = time.time()
    overlap_sec = max(60.0, float(cfg.settle_seconds * 3))
    fallback_floor = now - max(1, cfg.recent_files_hours) * 3600
    previous_watermark = db.get_meta_float("reconcile_watermark")
    scan_floor = max(0.0, fallback_floor if previous_watermark is None else previous_watermark - overlap_sec)
    newest_mtime = previous_watermark or 0.0
    for root in cfg.watch_roots:
        if not root.exists():
            continue
        for p in root.rglob("*"):
            if not p.is_file():
                continue
            if not is_candidate(p):
                continue
            try:
                st = p.stat()
            except FileNotFoundError:
                continue
            if float(st.st_mtime) < scan_floor:
                continue
            if db.upsert_candidate(p, cfg.settle_seconds, "reconcile"):
                count += 1
            newest_mtime = max(newest_mtime, float(st.st_mtime))
    db.set_meta("reconcile_watermark", f"{max(newest_mtime, now - overlap_sec):.6f}")
    return count


def ensure_message_job_tracking(
    db: StateDB,
    resolver: ClientResolver,
    media_resolver: Optional[WeChatDBResolver],
    cfg: Config,
    msg_ref: Optional[WeChatMessageRef],
    thumb_path: Optional[Path],
    client_source_path: Path,
    first_seen_at: float,
) -> Optional[sqlite3.Row]:
    if msg_ref is None or not msg_ref.msg_svr_id or not msg_ref.talker:
        return None
    if cfg.ui_force_scope == "mapped-groups" and not resolver.resolve(client_source_path):
        return None

    expected_image_path = msg_ref.image_abs_path or expected_full_image_from_thumb_path(thumb_path or client_source_path)
    if expected_image_path is None:
        return None

    talker_display = media_resolver.resolve_talker_display_name(msg_ref.talker) if media_resolver is not None else msg_ref.talker
    db.ensure_message_job(
        msg_svr_id=msg_ref.msg_svr_id,
        talker=msg_ref.talker,
        talker_display=talker_display or msg_ref.talker,
        thumb_path=thumb_path,
        expected_image_path=expected_image_path,
        create_time=msg_ref.create_time,
        first_seen_at=first_seen_at,
    )
    db.set_msg_cursor(msg_ref.create_time, msg_ref.msg_svr_id)
    return db.get_message_job(msg_ref.msg_svr_id)


class UIForceDownloadWorker(threading.Thread):
    def __init__(self, db: StateDB, cfg: Config, stop_event: threading.Event) -> None:
        super().__init__(name="wechat-ui-force-download", daemon=True)
        self.db = db
        self.cfg = cfg
        self.stop_event = stop_event
        self.available = UI_FORCE_DOWNLOADER_AVAILABLE and WeChatUIForceDownloader is not None
        self.unavailable_reason = None if self.available else (UI_FORCE_DOWNLOADER_IMPORT_ERROR or "ui_downloader_unavailable")
        self._downloader = (
            WeChatUIForceDownloader(
                focus_policy=cfg.ui_focus_policy,
                item_timeout_seconds=cfg.ui_item_timeout_seconds,
            )
            if self.available
            else None
        )

    def run(self) -> None:
        if not self.available or self._downloader is None:
            return

        while not self.stop_event.is_set():
            try:
                batch_id, jobs = self.db.claim_ui_batch()
            except Exception as exc:
                note = f"ui_claim_failed:{type(exc).__name__}:{exc}"
                self.db.set_meta("last_ui_result", note)
                print(f"[UI] claim_failed | err={note}")
                self.stop_event.wait(max(1.0, self.cfg.idle_sleep_seconds))
                continue
            if not batch_id or not jobs:
                self.stop_event.wait(max(0.5, self.cfg.idle_sleep_seconds))
                continue

            last_talker = str(jobs[0]["talker_display"])
            self.db.set_meta("last_ui_talker", last_talker)
            candidates = [
                UIMessageCandidate(
                    msg_svr_id=str(job["msg_svr_id"]),
                    talker=str(job["talker"]),
                    talker_display=str(job["talker_display"]),
                    expected_image_path=Path(str(job["expected_image_path"])),
                    thumb_path=Path(str(job["thumb_path"])),
                    create_time=float(job["create_time"]),
                )
                for job in jobs
            ]

            try:
                print(f"[UI] forcing_download | talker={last_talker} | items={len(candidates)}")
                result = self._downloader.force_download_batch(candidates)
                self.db.set_meta("last_ui_result", result.note)
                print(f"[UI] result | talker={last_talker} | ok={result.ok} | note={result.note} | resolved={len(result.resolved_msg_ids)}")
                self.db.finish_ui_batch(
                    batch_id,
                    result.resolved_msg_ids,
                    result.note,
                    self.cfg.ui_retry_backoff_seconds,
                    resolved_notes_by_msg_id=getattr(result, "resolved_sources", {}),
                )
                resolved_ids = set(result.resolved_msg_ids)
                for candidate in candidates:
                    if candidate.msg_svr_id in resolved_ids:
                        resolved_path_str = getattr(result, "resolved_media_paths", {}).get(candidate.msg_svr_id)
                        resolved_path = Path(resolved_path_str) if resolved_path_str else candidate.expected_image_path
                        if resolved_path.exists():
                            self.db.upsert_candidate(resolved_path, settle_seconds=1, source_event="ui-force")
                        elif candidate.expected_image_path.exists():
                            self.db.upsert_candidate(candidate.expected_image_path, settle_seconds=1, source_event="ui-force")
            except Exception as exc:
                note = f"ui_worker_failed:{type(exc).__name__}:{exc}"
                self.db.set_meta("last_ui_result", note)
                print(f"[UI] failed | talker={last_talker} | err={note}")
                self.db.finish_ui_batch(batch_id, [], note, self.cfg.ui_retry_backoff_seconds)


def has_core_signal(fields: dict[str, Any], bank: Optional[str]) -> bool:
    return any(
        value is not None and str(value).strip() != ""
        for value in (fields.get("amount"), fields.get("txn_date"), fields.get("txn_time"), bank)
    )


def get_prior_message_order_blocker(
    db: StateDB,
    msg_ref: Optional[WeChatMessageRef],
) -> Optional[sqlite3.Row]:
    if msg_ref is None:
        return None
    return db.find_prior_pending_message_job(
        talker=msg_ref.talker,
        create_time=float(msg_ref.create_time or 0.0),
        msg_svr_id=msg_ref.msg_svr_id,
    )


def resolve_media_candidate(
    item: QueueItem,
    db: StateDB,
    resolver: ClientResolver,
    media_resolver: Optional[WeChatDBResolver],
    cfg: Config,
) -> Optional[MediaResolution]:
    original_path = Path(item.path)
    original_source_kind = item.source_kind
    now = time.time()
    wait_deadline = item.first_seen + float(cfg.original_wait_seconds)
    ui_force_deadline = item.first_seen + float(cfg.ui_force_delay_seconds)

    if original_source_kind == "temp_image":
        context_path_str = db.find_recent_msgattach_context_path(
            item.mtime,
            lookback_sec=cfg.temp_correlation_seconds,
            lookahead_sec=15,
            limit=48,
        )
        if context_path_str is None:
            context_path_str = db.find_recent_unresolved_msgattach_context_path(
                max_age_sec=max(cfg.original_wait_seconds * 4, 1800),
                limit=60,
            )
        if context_path_str is None:
            if now < wait_deadline:
                db.mark_hold(item.file_id, reason="WAITING_TEMP_CONTEXT", delay_sec=10)
                print(f"[HOLD] {original_path.name} | waiting_temp_context")
            else:
                db.mark_done(item.file_id, sha256="", processed_at=now)
                print(f"[SKIP] {original_path.name} | temp_without_unique_context")
            return None

        client_source_path = Path(context_path_str)
        group_hash = extract_group_id_from_path(client_source_path)
        context_row = db.get_latest_file_row_by_path(client_source_path)
        context_mtime = float(context_row["mtime"]) if context_row is not None else item.mtime
        msg_ref = None
        if media_resolver is not None:
            msg_ref = media_resolver.find_message_for_path(client_source_path, context_mtime)
            if msg_ref is None and group_hash:
                msg_ref = media_resolver.find_unique_message_for_group(
                    group_hash,
                    context_mtime,
                    max(cfg.original_wait_seconds * 4, 1800),
                )
        resolved_client_source = msg_ref.preferred_context_path() if msg_ref is not None else None
        tracked_job = ensure_message_job_tracking(
            db=db,
            resolver=resolver,
            media_resolver=media_resolver,
            cfg=cfg,
            msg_ref=msg_ref,
            thumb_path=msg_ref.thumb_abs_path if msg_ref is not None else None,
            client_source_path=resolved_client_source or client_source_path,
            first_seen_at=item.first_seen,
        )
        order_blocker = get_prior_message_order_blocker(db, msg_ref)
        if order_blocker is not None:
            blocker_id = str(order_blocker["msg_svr_id"])
            db.mark_hold(item.file_id, reason=f"WAITING_PRIOR_MESSAGE_ORDER:{blocker_id}", delay_sec=10)
            print(f"[HOLD] {original_path.name} | waiting_prior_message_order | blocker={blocker_id}")
            return None
        if tracked_job is not None and msg_ref is not None and msg_ref.image_abs_path is not None and msg_ref.image_abs_path.exists():
            return MediaResolution(
                original_source_path=original_path,
                original_source_kind=original_source_kind,
                resolved_path=msg_ref.image_abs_path,
                resolved_source_kind=detect_source_kind(msg_ref.image_abs_path),
                client_source_path=resolved_client_source or client_source_path,
                resolution_source="db_image",
                verification_status="CONFIRMADO",
                msg_ref=msg_ref,
                using_thumb_fallback=False,
            )

        return MediaResolution(
            original_source_path=original_path,
            original_source_kind=original_source_kind,
            resolved_path=original_path,
            resolved_source_kind=detect_source_kind(original_path),
            client_source_path=resolved_client_source or client_source_path,
            resolution_source="temp_preview",
            verification_status="TEMP_PREVIEW",
            msg_ref=msg_ref,
            using_thumb_fallback=False,
        )

    msg_ref = media_resolver.find_message_for_path(original_path, item.mtime) if media_resolver is not None else None
    preferred_context = msg_ref.preferred_context_path() if msg_ref is not None else None
    client_source_path = preferred_context if preferred_context is not None else original_path
    tracked_job = ensure_message_job_tracking(
        db=db,
        resolver=resolver,
        media_resolver=media_resolver,
        cfg=cfg,
        msg_ref=msg_ref,
        thumb_path=original_path if original_source_kind == "msgattach_thumb_dat" else msg_ref.thumb_abs_path if msg_ref is not None else None,
        client_source_path=client_source_path,
        first_seen_at=item.first_seen,
    )
    order_blocker = get_prior_message_order_blocker(db, msg_ref)
    if order_blocker is not None:
        blocker_id = str(order_blocker["msg_svr_id"])
        db.mark_hold(item.file_id, reason=f"WAITING_PRIOR_MESSAGE_ORDER:{blocker_id}", delay_sec=10)
        print(f"[HOLD] {original_path.name} | waiting_prior_message_order | blocker={blocker_id}")
        return None

    if original_source_kind == "msgattach_thumb_dat":
        if msg_ref is not None and msg_ref.image_abs_path is not None and msg_ref.image_abs_path.exists():
            resolved = msg_ref.image_abs_path
            print(f"[INFO] {original_path.name} -> using_db_image={resolved.name}")
            return MediaResolution(
                original_source_path=original_path,
                original_source_kind=original_source_kind,
                resolved_path=resolved,
                resolved_source_kind=detect_source_kind(resolved),
                client_source_path=client_source_path,
                resolution_source="db_image",
                verification_status="CONFIRMADO",
                msg_ref=msg_ref,
                using_thumb_fallback=False,
            )

        sibling_image = resolve_full_image_from_thumb_path(original_path)
        if sibling_image is not None and sibling_image.exists():
            print(f"[INFO] {original_path.name} -> using_sibling_image={sibling_image.name}")
            return MediaResolution(
                original_source_path=original_path,
                original_source_kind=original_source_kind,
                resolved_path=sibling_image,
                resolved_source_kind=detect_source_kind(sibling_image),
                client_source_path=client_source_path,
                resolution_source="path_sibling_image",
                verification_status="CONFIRMADO",
                msg_ref=msg_ref,
                using_thumb_fallback=False,
            )

        tracked_state = str(tracked_job["state"] or "") if tracked_job is not None else ""
        if tracked_job is not None and tracked_state == "UI_FORCE_RUNNING" and now < wait_deadline:
            db.mark_hold(item.file_id, reason="WAITING_UI_FORCE_DOWNLOAD", delay_sec=10)
            print(f"[HOLD] {original_path.name} | waiting_ui_force_download_running")
            return None

        if tracked_job is not None and now < ui_force_deadline:
            db.set_message_job_state(msg_ref.msg_svr_id if msg_ref is not None else None, "WAITING_ORIGINAL", note="WAITING_ORIGINAL_MEDIA", next_ui_attempt_at=0.0, reset_batch=True)
            db.mark_hold(item.file_id, reason="WAITING_ORIGINAL_MEDIA", delay_sec=10)
            print(f"[HOLD] {original_path.name} | waiting_original_media")
            return None

        if tracked_job is not None and cfg.ui_force_download_enabled and now < wait_deadline:
            remaining = max(5, min(15, int(wait_deadline - now)))
            db.set_message_job_state(msg_ref.msg_svr_id if msg_ref is not None else None, "UI_FORCE_PENDING", note="WAITING_UI_FORCE_DOWNLOAD", next_ui_attempt_at=0.0, reset_batch=True)
            db.mark_hold(item.file_id, reason="WAITING_UI_FORCE_DOWNLOAD", delay_sec=remaining)
            print(f"[HOLD] {original_path.name} | waiting_ui_force_download")
            return None

        if now < wait_deadline:
            db.mark_hold(item.file_id, reason="WAITING_ORIGINAL_MEDIA", delay_sec=10)
            print(f"[HOLD] {original_path.name} | waiting_original_media")
            return None

        if tracked_job is not None and cfg.ui_force_download_enabled and tracked_state != "UI_FORCE_RUNNING":
            db.set_message_job_state(
                msg_ref.msg_svr_id if msg_ref is not None else None,
                "UI_FORCE_PENDING",
                note="WAITING_ORIGINAL_MEDIA",
                next_ui_attempt_at=0.0,
                reset_batch=True,
            )
        elif tracked_job is not None:
            db.set_message_job_state(
                msg_ref.msg_svr_id if msg_ref is not None else None,
                "WAITING_ORIGINAL",
                note="WAITING_ORIGINAL_MEDIA",
                next_ui_attempt_at=0.0,
                reset_batch=True,
            )
        db.mark_hold(item.file_id, reason="WAITING_ORIGINAL_MEDIA", delay_sec=15)
        print(f"[HOLD] {original_path.name} | waiting_original_media_no_thumb_fallback")
        return None

    resolved_path = original_path
    resolved_source_kind = original_source_kind
    resolution_source = "direct_image"
    if msg_ref is not None and msg_ref.image_abs_path is not None and msg_ref.image_abs_path.exists():
        resolved_path = msg_ref.image_abs_path
        resolved_source_kind = detect_source_kind(resolved_path)
        resolution_source = "db_image"

    return MediaResolution(
        original_source_path=original_path,
        original_source_kind=original_source_kind,
        resolved_path=resolved_path,
        resolved_source_kind=resolved_source_kind,
        client_source_path=client_source_path,
        resolution_source=resolution_source,
        verification_status="CONFIRMADO",
        msg_ref=msg_ref,
        using_thumb_fallback=False,
    )


def process_item(
    item: QueueItem,
    db: StateDB,
    sink: RowSink,
    ocr: OCREngine,
    resolver: ClientResolver,
    media_resolver: Optional[WeChatDBResolver],
    cfg: Config,
) -> None:
    if db.receipt_exists(item.file_id):
        db.mark_done(item.file_id, sha256="", processed_at=time.time())
        return

    path = Path(item.path)
    resolution: Optional[MediaResolution] = None
    msg_svr_id: Optional[str] = None
    try:
        if media_resolver is not None:
            media_resolver.refresh_if_due()

        resolution = resolve_media_candidate(item=item, db=db, resolver=resolver, media_resolver=media_resolver, cfg=cfg)
        if resolution is None:
            return

        msg_svr_id = resolution.msg_ref.msg_svr_id if resolution.msg_ref is not None else None
        if db.receipt_msg_exists(msg_svr_id):
            db.mark_done(item.file_id, sha256="", processed_at=time.time(), note="RESOLVED_BY_LATER_SUCCESS")
            db.resolve_message_job_paths(msg_svr_id, exclude_file_id=item.file_id, sha256="")
            db.mark_message_job_resolved(msg_svr_id, note="DUPLICATE_MSG_SVR_ID")
            path = resolution.resolved_path
            print(f"[SKIP] {path.name} | duplicate_msg_svr_id={msg_svr_id}")
            return

        path = resolution.resolved_path
        if not path.exists():
            if item.source_kind == "temp_image" and db.message_job_is_terminal(msg_svr_id):
                db.mark_done(item.file_id, sha256="", processed_at=time.time(), note="RESOLVED_BY_LATER_SUCCESS")
                print(f"[SKIP] {path.name} | temp_resolved_by_later_success")
                return
            raise FileNotFoundError(f"Resolved file disappeared: {path}")

        client = resolver.resolve(resolution.client_source_path)
        if not client:
            gid = extract_group_id_from_path(resolution.client_source_path) or "SEM_GRUPO"
            db.mark_hold(item.file_id, reason=f"MISSING_CLIENT_MAP:{gid}", delay_sec=120)
            print(f"[HOLD] {path.name} | grupo_sem_mapa={gid}")
            return

        img, img_bytes, _ext, _key = open_image_from_file(path)
        digest = sha256_bytes(img_bytes)
        if db.receipt_sha_exists(digest):
            db.mark_done(item.file_id, sha256=digest, processed_at=time.time())
            db.mark_message_job_resolved(msg_svr_id, note="DUPLICATE_SHA")
            print(f"[SKIP] {path.name} | duplicate_sha")
            return
        q_score = quality_score(img)

        img_for_ocr = prepare_image_for_ocr(img, resolution.resolved_source_kind)
        text, ocr_conf = ocr.extract(img_for_ocr)
        ocr_chars = len(text)
        is_receipt, receipt_reason = looks_like_single_receipt(text)
        if not is_receipt:
            db.mark_done(item.file_id, sha256=digest, processed_at=time.time())
            db.mark_message_job_resolved(msg_svr_id, note=f"NOT_RECEIPT:{receipt_reason}")
            print(f"[SKIP] {path.name} | not_receipt={receipt_reason}")
            return

        fields = parse_receipt_fields(text, ocr_conf=ocr_conf, q_score=q_score)
        bank = fields.get("bank")
        if bank is None:
            bank = detect_bank(f"{text}\n{client}", fields.get("beneficiary"))
            fields["bank"] = bank
        if not has_core_signal(fields, bank):
            db.mark_exception(item.file_id, reason="EXCEPTION_MISSING_CORE_FIELDS")
            db.set_meta("last_exception_reason", "EXCEPTION_MISSING_CORE_FIELDS")
            db.mark_message_job_exception(msg_svr_id, note="EXCEPTION_MISSING_CORE_FIELDS")
            print(f"[EXCEPTION] {path.name} | missing_core_fields | verification={resolution.verification_status}")
            return

        quality_floor = 0.20 if resolution.using_thumb_fallback else 0.38
        conf_floor = max(cfg.min_confidence, 0.70) if resolution.using_thumb_fallback else cfg.min_confidence
        review_needed = (
            fields["amount"] is None
            or fields["txn_date"] is None
            or fields["txn_time"] is None
            or bank is None
            or fields["parse_conf"] < conf_floor
            or q_score < quality_floor
            or resolution.verification_status != "CONFIRMADO"
        )

        payload: dict[str, Any] = {
            "file_id": item.file_id,
            "source_path": str(resolution.original_source_path),
            "source_kind": resolution.original_source_kind,
            "ingested_at": time.time(),
            "sha256": digest,
            "txn_date": fields["txn_date"],
            "txn_time": fields["txn_time"],
            "client": client,
            "bank": bank,
            "beneficiary": fields["beneficiary"],
            "amount": fields["amount"],
            "currency": fields["currency"],
            "parse_conf": fields["parse_conf"],
            "quality_score": q_score,
            "ocr_engine": ocr.name,
            "ocr_conf": ocr_conf,
            "ocr_chars": ocr_chars,
            "review_needed": review_needed,
            "ocr_text": text[:25000] if text else "",
            "parser_json": json.dumps(fields, ensure_ascii=False),
            "msg_svr_id": msg_svr_id,
            "talker": resolution.msg_ref.talker if resolution.msg_ref is not None else None,
            "resolved_media_path": str(path),
            "resolution_source": resolution.resolution_source,
            "verification_status": resolution.verification_status,
            "error": None,
        }

        sheet, row = sink.append(payload, review_needed=review_needed)
        payload["excel_sheet"] = sheet
        payload["excel_row"] = row
        db.insert_receipt(payload)
        db.mark_done(item.file_id, sha256=digest, processed_at=time.time())
        db.resolve_related_file_paths(
            source_path=resolution.original_source_path,
            exclude_file_id=item.file_id,
            sha256=digest,
        )
        db.resolve_message_job_paths(msg_svr_id, exclude_file_id=item.file_id, sha256=digest)
        if resolution.verification_status == "THUMB_FALLBACK":
            db.mark_message_job_thumb_fallback(msg_svr_id, note=resolution.resolution_source)
        else:
            db.mark_message_job_resolved(msg_svr_id, note=resolution.resolution_source)
        db.set_meta("last_resolution_source", resolution.resolution_source)
        db.set_meta("last_verification_status", resolution.verification_status)

        print(
            f"[OK] {path.name} | cliente={client} | banco={bank} | valor={fields['amount']} "
            f"| data={fields['txn_date']} {fields['txn_time']} | sheet={sheet} "
            f"| resolution={resolution.resolution_source} | verification={resolution.verification_status}"
        )

    except Exception as exc:
        if isinstance(exc, FileNotFoundError) and item.source_kind == "temp_image" and db.message_job_is_terminal(msg_svr_id):
            db.mark_done(item.file_id, sha256="", processed_at=time.time(), note="RESOLVED_BY_LATER_SUCCESS")
            print(f"[SKIP] {path.name} | temp_missing_after_resolution")
            return
        if isinstance(exc, FileNotFoundError) and item.source_kind == "temp_image" and item.attempts >= 3:
            db.mark_done(item.file_id, sha256="", processed_at=time.time(), note="STALE_TEMP_ORPHAN")
            print(f"[SKIP] {path.name} | temp_file_disappeared_after_{item.attempts}_attempts")
            return
        fast_retry = 5 if isinstance(exc, PermissionError) else None
        db.mark_retry(
            file_id=item.file_id,
            attempts=item.attempts,
            retry_base_sec=cfg.retry_base_seconds,
            err=f"{type(exc).__name__}: {exc}",
            max_retries=cfg.max_retries,
            delay_override_sec=fast_retry,
        )
        print(f"[RETRY] {path.name} | attempt={item.attempts} | err={type(exc).__name__}: {exc}")


def default_watch_roots() -> list[Path]:
    home = Path(os.environ.get("USERPROFILE", str(Path.home())))
    base = home / "Documents" / "WeChat Files"
    roots: list[Path] = []
    if base.exists():
        for sub in base.iterdir():
            fs = sub / "FileStorage"
            if fs.exists():
                roots.append(fs)
    return roots


def parse_boolish(value: Any, default: bool = False) -> bool:
    if value is None:
        return default
    text = str(value).strip().lower()
    if not text:
        return default
    if text in {"1", "true", "yes", "y", "on"}:
        return True
    if text in {"0", "false", "no", "n", "off"}:
        return False
    return default


def parse_retry_backoff_seconds(value: Any) -> list[int]:
    if isinstance(value, list):
        raw_parts = value
    else:
        raw_parts = re.split(r"[\s,;]+", str(value or "").strip())

    out: list[int] = []
    for part in raw_parts:
        try:
            ivalue = int(part)
        except Exception:
            continue
        if ivalue > 0:
            out.append(ivalue)
    return out or [5, 10, 20, 40]


def ensure_client_map_file(map_path: Path, watch_roots: list[Path]) -> None:
    if map_path.exists():
        return
    map_path.parent.mkdir(parents=True, exist_ok=True)

    discovered: dict[str, str] = {}
    for root in watch_roots:
        msgattach = root / "MsgAttach"
        if not msgattach.exists():
            continue
        for sub in msgattach.iterdir():
            if not sub.is_dir():
                continue
            gid = sub.name.strip()
            if gid and gid.lower() not in discovered:
                discovered[gid.lower()] = ""
            if len(discovered) >= 30:
                break
        if len(discovered) >= 30:
            break

    template: dict[str, str] = {
        "COLE_AQUI_ID_DO_GRUPO": "NOME_DO_CLIENTE",
    }
    for gid in sorted(discovered.keys()):
        template[gid] = ""
    map_path.write_text(json.dumps(template, ensure_ascii=False, indent=2), encoding="utf-8")


def parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(description="WeChat receipt ingestion daemon")
    p.add_argument("--watch-root", action="append", default=[], help="Root directory to monitor (repeatable)")
    p.add_argument("--db-path", default=str(Path.cwd() / "wechat_receipt_state.db"))
    p.add_argument("--excel-path", default=str(Path.cwd() / "pagamentos_wechat.xlsx"))
    p.add_argument("--sink-mode", choices=("excel", "google-sheets"), default=os.getenv("WECHAT_SINK_MODE", "excel"))
    p.add_argument("--gsheet-ref", default=os.getenv("WECHAT_GSHEET_REF", ""))
    p.add_argument("--gsheet-worksheet", default=os.getenv("WECHAT_GSHEET_WORKSHEET", ""))
    p.add_argument("--gsheet-review-worksheet", default=os.getenv("WECHAT_GSHEET_REVIEW_WORKSHEET", "Revisar"))
    p.add_argument("--google-credentials-path", default=os.getenv("WECHAT_GOOGLE_CREDENTIALS_PATH", ""))
    p.add_argument("--verification-column-name", default=os.getenv("WECHAT_VERIFICATION_COLUMN_NAME", DEFAULT_VERIFICATION_COLUMN_NAME))
    p.add_argument("--client-map-path", default=str(Path.cwd() / "clientes_grupos.json"))
    p.add_argument("--resolution-mode", choices=("path-only", "db-first"), default=os.getenv("WECHAT_RESOLUTION_MODE", "db-first"))
    p.add_argument("--db-merge-path", default=os.getenv("WECHAT_DB_MERGE_PATH", str(Path.cwd() / ".runtime" / "wechat_merge.db")))
    p.add_argument("--settle-seconds", type=int, default=5)
    p.add_argument("--reconcile-seconds", type=int, default=90)
    p.add_argument("--recent-files-hours", type=int, default=int(os.getenv("WECHAT_RECENT_FILES_HOURS", "24")))
    p.add_argument("--idle-sleep-seconds", type=float, default=1.2)
    p.add_argument("--retry-base-seconds", type=int, default=30)
    p.add_argument("--min-confidence", type=float, default=0.55)
    p.add_argument("--max-retries", type=int, default=0, help="0 means infinite retries")
    p.add_argument("--original-wait-seconds", type=int, default=int(os.getenv("WECHAT_ORIGINAL_WAIT_SECONDS", "90")))
    p.add_argument("--temp-correlation-seconds", type=int, default=int(os.getenv("WECHAT_TEMP_CORRELATION_SECONDS", "30")))
    p.add_argument("--ui-force-download-enabled", default=os.getenv("WECHAT_UI_FORCE_DOWNLOAD_ENABLED", "true"))
    p.add_argument("--ui-force-delay-seconds", type=int, default=int(os.getenv("WECHAT_UI_FORCE_DELAY_SECONDS", "15")))
    p.add_argument("--ui-force-scope", default=os.getenv("WECHAT_UI_FORCE_SCOPE", "mapped-groups"))
    p.add_argument("--ui-focus-policy", default=os.getenv("WECHAT_UI_FOCUS_POLICY", "immediate"))
    p.add_argument("--ui-batch-mode", default=os.getenv("WECHAT_UI_BATCH_MODE", "group-sequential"))
    p.add_argument("--ui-item-timeout-seconds", type=int, default=int(os.getenv("WECHAT_UI_ITEM_TIMEOUT_SECONDS", "5")))
    p.add_argument("--ui-retry-backoff-seconds", default=os.getenv("WECHAT_UI_RETRY_BACKOFF_SECONDS", "5,10,20,40"))
    p.add_argument("--disable-watchdog", action="store_true")
    return p.parse_args()


def build_config(args: argparse.Namespace) -> Config:
    roots = [Path(r) for r in args.watch_root] if args.watch_root else default_watch_roots()
    return Config(
        watch_roots=roots,
        db_path=Path(args.db_path),
        db_merge_path=Path(args.db_merge_path),
        excel_path=Path(args.excel_path),
        sink_mode=str(args.sink_mode).strip().lower(),
        gsheet_ref=(str(args.gsheet_ref).strip() or None),
        gsheet_worksheet=(str(args.gsheet_worksheet).strip() or None),
        gsheet_review_worksheet=(str(args.gsheet_review_worksheet).strip() or None),
        google_credentials_path=(Path(args.google_credentials_path) if str(args.google_credentials_path).strip() else None),
        verification_column_name=(str(args.verification_column_name).strip() or DEFAULT_VERIFICATION_COLUMN_NAME),
        client_map_path=Path(args.client_map_path),
        resolution_mode=(str(args.resolution_mode).strip().lower() or "db-first"),
        settle_seconds=max(1, args.settle_seconds),
        reconcile_seconds=max(20, args.reconcile_seconds),
        recent_files_hours=max(1, int(args.recent_files_hours)),
        idle_sleep_seconds=max(0.2, args.idle_sleep_seconds),
        retry_base_seconds=max(10, args.retry_base_seconds),
        min_confidence=max(0.0, min(1.0, args.min_confidence)),
        max_retries=max(0, args.max_retries),
        original_wait_seconds=max(30, int(args.original_wait_seconds)),
        temp_correlation_seconds=max(5, int(args.temp_correlation_seconds)),
        ui_force_download_enabled=parse_boolish(args.ui_force_download_enabled, default=True),
        ui_force_delay_seconds=max(5, int(args.ui_force_delay_seconds)),
        ui_force_scope=(str(args.ui_force_scope).strip().lower() or "mapped-groups"),
        ui_focus_policy=(str(args.ui_focus_policy).strip().lower() or "immediate"),
        ui_batch_mode=(str(args.ui_batch_mode).strip().lower() or "group-sequential"),
        ui_item_timeout_seconds=max(1, int(args.ui_item_timeout_seconds)),
        ui_retry_backoff_seconds=parse_retry_backoff_seconds(args.ui_retry_backoff_seconds),
        disable_watchdog=bool(args.disable_watchdog),
    )


def build_sink(cfg: Config) -> RowSink:
    if cfg.sink_mode == "google-sheets":
        if not cfg.gsheet_ref:
            raise RuntimeError("Google Sheets mode requires --gsheet-ref with the sheet URL or spreadsheet id")
        return GoogleSheetsSink(
            spreadsheet_ref=cfg.gsheet_ref,
            credentials_path=cfg.google_credentials_path,
            main_worksheet=cfg.gsheet_worksheet,
            review_worksheet=cfg.gsheet_review_worksheet,
            verification_column_name=cfg.verification_column_name,
        )
    return ExcelSink(cfg.excel_path, verification_column_name=cfg.verification_column_name)


def main() -> int:
    args = parse_args()
    cfg = build_config(args)

    # Avoid Windows cp1252 crashes when group names contain non-Latin chars.
    try:
        if hasattr(sys.stdout, "reconfigure"):
            sys.stdout.reconfigure(encoding="utf-8", errors="replace")
        if hasattr(sys.stderr, "reconfigure"):
            sys.stderr.reconfigure(encoding="utf-8", errors="replace")
    except Exception:
        pass

    if not cfg.watch_roots:
        print("No watch roots found. Pass --watch-root explicitly.")
        return 2

    print("Watch roots:")
    for r in cfg.watch_roots:
        print(f" - {r}")
    print(f"DB: {cfg.db_path}")
    print(f"Sink mode: {cfg.sink_mode}")
    if cfg.sink_mode == "excel":
        print(f"Excel: {cfg.excel_path}")
    else:
        print(f"Google Sheets ref: {cfg.gsheet_ref}")
        if cfg.google_credentials_path:
            print(f"Google credentials: {cfg.google_credentials_path}")
    print(f"Client map: {cfg.client_map_path}")
    print(f"Recent files window (hours): {cfg.recent_files_hours}")
    print(f"Resolution mode: {cfg.resolution_mode}")
    print(f"DB merge path: {cfg.db_merge_path}")
    print(f"Original wait (seconds): {cfg.original_wait_seconds}")
    print(f"Temp correlation (seconds): {cfg.temp_correlation_seconds}")
    print(f"Verification column: {cfg.verification_column_name}")
    print(f"UI force download: {cfg.ui_force_download_enabled}")
    print(f"UI force delay (seconds): {cfg.ui_force_delay_seconds}")
    print(f"UI force scope: {cfg.ui_force_scope}")
    print(f"UI focus policy: {cfg.ui_focus_policy}")
    print(f"UI batch mode: {cfg.ui_batch_mode}")
    print(f"UI item timeout (seconds): {cfg.ui_item_timeout_seconds}")
    print(f"UI retry backoff (seconds): {cfg.ui_retry_backoff_seconds}")
    if cfg.ui_force_download_enabled:
        if not UI_FORCE_DOWNLOADER_AVAILABLE or WeChatUIForceDownloader is None:
            err = UI_FORCE_DOWNLOADER_IMPORT_ERROR or "ui_downloader_unavailable"
            print(f"[WARN] UI probe failed | err={err}")
        else:
            try:
                ui_probe = WeChatUIForceDownloader(
                    focus_policy=cfg.ui_focus_policy,
                    item_timeout_seconds=cfg.ui_item_timeout_seconds,
                )
                probe_ok, probe_note = ui_probe.probe_main_window()
                if probe_ok:
                    print("UI probe: ok")
                else:
                    print(f"[WARN] UI probe failed | err={probe_note}")
            except Exception as exc:
                print(f"[WARN] UI probe failed | err={type(exc).__name__}: {exc}")

    ensure_client_map_file(cfg.client_map_path, cfg.watch_roots)
    resolver = ClientResolver(cfg.client_map_path)
    media_resolver: Optional[WeChatDBResolver] = None
    if cfg.resolution_mode == "db-first":
        media_resolver = WeChatDBResolver(cfg.watch_roots, cfg.db_merge_path, refresh_seconds=10)

    db = StateDB(cfg.db_path)
    ignored_old = db.ignore_stale_queue(time.time() - max(1, cfg.recent_files_hours) * 3600)
    if ignored_old:
        print(f"[RECOVER] ignored_old_queue={ignored_old} | older_than_hours={cfg.recent_files_hours}")
    cleaned_temp_orphans = db.cleanup_stale_temp_orphans(max_age_sec=max(600, cfg.original_wait_seconds * 4))
    if cleaned_temp_orphans:
        print(f"[RECOVER] stale_temp_orphans={cleaned_temp_orphans}")
    try:
        sink = build_sink(cfg)
    except Exception as exc:
        print(str(exc))
        return 4

    if isinstance(sink, GoogleSheetsSink):
        main_sheet = sink._main_sheet_title or sink._resolve_main_sheet_title()
        review_sheet = sink.review_worksheet or main_sheet
        label = sink.spreadsheet_title or sink.spreadsheet_id
        print(
            f"Google Sheets: {label} | main={main_sheet} | review={review_sheet} | auth={sink.auth_mode}"
        )
    if media_resolver is not None:
        if media_resolver.refresh_if_due(force=True):
            print(f"WeChat DB resolver: enabled | wx_dir={media_resolver.selected_wx_dir} | merge={cfg.db_merge_path}")
        else:
            print(f"WeChat DB resolver: degraded_to_path_only | err={media_resolver.last_error or 'unknown'}")

    requeued = db.requeue_mapped_missing_client(resolver, max_age_hours=3, limit=1200)
    if requeued:
        print(f"[RECOVER] requeued_missing_client={requeued}")
    backfilled = db.backfill_receipt_context(resolver, limit=8000)
    if backfilled:
        print(f"[RECOVER] backfilled_receipt_context={backfilled}")
    try:
        ocr = build_ocr_engine()
    except Exception as exc:
        print(str(exc))
        return 3
    print(f"OCR engine: {ocr.name}")

    observer: Optional[Observer] = None
    if WATCHDOG_AVAILABLE and not cfg.disable_watchdog:
        observer = Observer()
        handler = IngestEventHandler(db, cfg.settle_seconds)
        for root in cfg.watch_roots:
            if root.exists():
                observer.schedule(handler, str(root), recursive=True)
        observer.start()
        print("Watchdog: enabled")
    else:
        print("Watchdog: disabled (using reconcile polling only)")

    stop_event = threading.Event()
    ui_worker: Optional[UIForceDownloadWorker] = None
    if cfg.ui_force_download_enabled and cfg.resolution_mode == "db-first":
        ui_worker = UIForceDownloadWorker(db=db, cfg=cfg, stop_event=stop_event)
        if ui_worker.available:
            ui_worker.start()
            print("UI force download worker: enabled")
        else:
            print(f"UI force download worker: unavailable | err={ui_worker.unavailable_reason}")
    else:
        print("UI force download worker: disabled")

    last_reconcile = 0.0
    try:
        while True:
            now = time.time()
            if now - last_reconcile >= cfg.reconcile_seconds:
                added = reconcile_scan(cfg, db)
                last_reconcile = now
                print(f"[SCAN] reconcile complete | queued_or_refreshed={added}")

            item = db.claim_next()
            if item is None:
                time.sleep(cfg.idle_sleep_seconds)
                continue
            process_item(
                item=item,
                db=db,
                sink=sink,
                ocr=ocr,
                resolver=resolver,
                media_resolver=media_resolver,
                cfg=cfg,
            )
    except KeyboardInterrupt:
        print("Stopping daemon...")
    finally:
        stop_event.set()
        if ui_worker is not None and ui_worker.is_alive():
            ui_worker.join(timeout=5)
        if observer is not None:
            observer.stop()
            observer.join(timeout=5)
        db.close()
    return 0


if __name__ == "__main__":
    sys.exit(main())
