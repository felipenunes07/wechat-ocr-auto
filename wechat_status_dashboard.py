from __future__ import annotations

import json
import os
import sqlite3
import subprocess
import time
from collections import deque
from dataclasses import dataclass, field
from datetime import datetime
from pathlib import Path
from typing import Any, Optional

import tkinter as tk
from tkinter import messagebox, ttk


APP_TITLE = "Painel WeChat OCR"
REFRESH_SECONDS = 5
MAX_QUEUE_ROWS = 120
MAX_MESSAGE_ROWS = 120
MAX_RECEIPT_ROWS = 40
MAX_LOG_LINES = 24
UI_FORCE_RUNTIME_META_KEY = "ui_force_runtime_enabled"
MANUAL_SESSION_META_KEY = "manual_session_started_at"
SINK_CONFIG_FILE = "sink_config.json"
IGNORED_BY_USER_STATE = "IGNORED_BY_USER"
IGNORE_ITEM_REASON = "IGNORED_BY_USER_ITEM"
IGNORE_QUEUE_REASON = "IGNORED_BY_USER_CLEAR_QUEUE"
RELEASED_AFTER_IGNORE_REASON = "RELEASED_AFTER_USER_IGNORE"

ACTIVE_QUEUE_FILE_STATUSES = ("pending", "retry", "processing", "exception", "failed")
PENDING_RECEIPT_STATUSES = ("SINK_PENDING", "SINK_BLOCKED_PRIOR_MSG", "SINK_RETRY", "SINK_RUNNING")
WAITING_IMAGE_CODES = {
    "WAITING_ORIGINAL_MEDIA",
    "MANUAL_WAIT_ORIGINAL",
    "WAITING_TEMP_CONTEXT",
    "WAITING_TEMP_DB_MATCH",
}
ORDER_BLOCK_PREFIXES = (
    "WAITING_PRIOR_MESSAGE_ORDER:",
    "WAITING_SESSION_PRIOR_MESSAGE_ORDER:",
)
QUEUE_FILTERS = (
    ("all", "Todos"),
    ("waiting_image", "Esperando imagem"),
    ("blocked", "Bloqueadas"),
    ("wechat", "WeChat"),
    ("failures", "Falhas"),
)
METRIC_SPECS = (
    ("Na fila", "#1d4ed8"),
    ("Esperando imagem", "#f59e0b"),
    ("Bloqueadas por ordem", "#dc2626"),
    ("Aguardando WeChat", "#7c3aed"),
    ("WeChat em acao", "#0f766e"),
    ("Processando", "#166534"),
    ("Precisam atencao", "#b42318"),
    ("Recebidos 24h", "#0f172a"),
)
STATUS_HELP_ROWS = (
    ("UI pendente", "O sistema vai tentar abrir ou baixar a imagem pelo WeChat."),
    ("UI rodando", "O WeChat esta sendo usado agora para buscar a imagem original."),
    ("Esperando imagem original", "A miniatura chegou, mas a imagem completa ainda nao apareceu."),
    ("Bloqueada por mensagem anterior", "Existe uma mensagem mais antiga do mesmo grupo que precisa terminar antes."),
)
PALETTE = {
    "bg": "#eef4fb",
    "surface": "#ffffff",
    "surface_alt": "#f8fbff",
    "border": "#d6e0ea",
    "text": "#10243a",
    "muted": "#5c7086",
    "header": "#0f1f38",
    "header_muted": "#b9c7d8",
    "danger_bg": "#7f1d1d",
    "danger_alt": "#991b1b",
    "success": "#166534",
    "warning": "#b45309",
    "info": "#1d4ed8",
}

WAITING_LABELS = {
    "WAITING_ORIGINAL_MEDIA": "Esperando imagem original",
    "WAITING_UI_FORCE_DOWNLOAD": "Aguardando WeChat",
    "WAITING_TEMP_CONTEXT": "Esperando contexto da conversa",
    "WAITING_TEMP_DB_MATCH": "Tentando vincular a mensagem",
    "MANUAL_WAIT_ORIGINAL": "Esperando abrir no PC",
    "UI_FORCE_DISABLED_MANUAL_MODE": "Modo manual ativado",
    "EXCEPTION_MISSING_CORE_FIELDS": "Falhou ao ler dados principais",
    "STALE_TEMP_ORPHAN": "Preview antigo descartado",
    "RESOLVED_BY_LATER_SUCCESS": "Resolvida por outra imagem",
    IGNORE_ITEM_REASON: "Ignorada por voce",
    IGNORE_QUEUE_REASON: "Ignorada na limpeza da fila",
    RELEASED_AFTER_IGNORE_REASON: "Liberada apos ignorar bloqueio",
}

MESSAGE_STATE_LABELS = {
    "NEW": "Nova",
    "SESSION_PENDING_OPEN": "Nova",
    "WAITING_ORIGINAL": "Esperando imagem original",
    "UI_FORCE_PENDING": "Aguardando WeChat",
    "UI_FORCE_RUNNING": "WeChat buscando",
    "RESOLVED": "Concluida",
    "THUMB_FALLBACK": "Processada pela miniatura",
    "EXCEPTION": "Falhou",
    "IGNORED_SESSION_ROLLOVER": "Ignorada",
    "IGNORED_STALE_MANUAL_SESSION": "Ignorada",
    IGNORED_BY_USER_STATE: "Ignorada",
}

FILE_STATUS_LABELS = {
    "pending": "Na fila",
    "retry": "Aguardando nova tentativa",
    "processing": "Processando",
    "exception": "Falhou",
    "failed": "Falhou",
    "done": "Concluida",
    "ignored": "Ignorada",
}

SOURCE_KIND_LABELS = {
    "msgattach_image_dat": "Imagem original",
    "msgattach_image_plain": "Imagem original",
    "msgattach_thumb_dat": "Miniatura",
    "temp_image": "Preview temp",
}


@dataclass
class DashboardSnapshot:
    daemon_status: str
    daemon_running: bool
    metrics: dict[str, str] = field(default_factory=dict)
    queue_rows: list[dict[str, Any]] = field(default_factory=list)
    message_rows: list[dict[str, Any]] = field(default_factory=list)
    receipt_rows: list[dict[str, Any]] = field(default_factory=list)
    log_lines: list[str] = field(default_factory=list)
    last_ui_result: str = "-"
    last_ui_talker: str = "-"
    last_exception: str = "-"
    last_resolution: str = "-"
    last_verification: str = "-"
    ui_force_runtime_enabled: bool = False
    error: str = ""


def fmt_dt(ts: Optional[float]) -> str:
    if not ts:
        return "-"
    try:
        return datetime.fromtimestamp(float(ts)).strftime("%d/%m %H:%M:%S")
    except Exception:
        return "-"


def fmt_age(ts: Optional[float]) -> str:
    if not ts:
        return "-"
    try:
        delta = max(0, int(time.time() - float(ts)))
    except Exception:
        return "-"
    if delta < 60:
        return f"{delta}s"
    minutes, seconds = divmod(delta, 60)
    if minutes < 60:
        return f"{minutes}m {seconds:02d}s"
    hours, minutes = divmod(minutes, 60)
    if hours < 24:
        return f"{hours}h {minutes:02d}m"
    days, hours = divmod(hours, 24)
    return f"{days}d {hours:02d}h"


def short_text(value: Any, limit: int = 72) -> str:
    text = " ".join(str(value or "").strip().split())
    if not text:
        return "-"
    if len(text) <= limit:
        return text
    return text[: limit - 3] + "..."


def compact_path(value: Any, limit: int = 72) -> str:
    raw = str(value or "").strip()
    if not raw:
        return "-"
    path = Path(raw)
    if len(path.name) <= limit:
        return path.name
    return raw[-limit:]


def is_order_wait_reason(last_error: Any) -> bool:
    err = str(last_error or "").strip()
    return any(err.startswith(prefix) for prefix in ORDER_BLOCK_PREFIXES)


def wait_reason_label(last_error: Any) -> str:
    err = str(last_error or "").strip()
    if not err:
        return "-"
    if is_order_wait_reason(err):
        return "Bloqueada por mensagem anterior"
    if err.startswith("WAITING_PRIOR_SINK_SESSION_MESSAGE:") or err.startswith("WAITING_PRIOR_SINK_RECEIPT:"):
        return "Bloqueada por envio anterior"
    if err.startswith("MISSING_CLIENT_MAP:"):
        return "Grupo sem cliente mapeado"
    if err.startswith("IGNORED_"):
        return "Ignorada"
    if err.startswith("EXCEPTION_"):
        return WAITING_LABELS.get(err, "Falhou")
    return WAITING_LABELS.get(err, short_text(err, limit=58))


def message_state_label(state: Any) -> str:
    raw = str(state or "").strip()
    if not raw:
        return "-"
    return MESSAGE_STATE_LABELS.get(raw, raw.replace("_", " ").title())


def file_status_label(status: Any) -> str:
    raw = str(status or "").strip().lower()
    if not raw:
        return "-"
    return FILE_STATUS_LABELS.get(raw, raw.replace("_", " "))


def source_kind_label(kind: Any) -> str:
    raw = str(kind or "").strip()
    if not raw:
        return "-"
    return SOURCE_KIND_LABELS.get(raw, raw.replace("_", " "))


def queue_filter_bucket(row: dict[str, Any]) -> str:
    file_status = str(row.get("raw_file_status") or "").strip().lower()
    wait_code = str(row.get("raw_wait_code") or "").strip()
    message_state = str(row.get("raw_message_state") or "").strip()
    technical = str(row.get("technical_code") or "").strip()
    if file_status in {"exception", "failed"} or message_state == "EXCEPTION" or technical.startswith("file=exception"):
        return "failures"
    if is_order_wait_reason(wait_code) or wait_code.startswith("WAITING_PRIOR_SINK_"):
        return "blocked"
    if wait_code == "WAITING_UI_FORCE_DOWNLOAD" or message_state in {"UI_FORCE_PENDING", "UI_FORCE_RUNNING"}:
        return "wechat"
    if wait_code in WAITING_IMAGE_CODES or message_state == "WAITING_ORIGINAL":
        return "waiting_image"
    return "all"


def row_tag_for_bucket(bucket: str, row: dict[str, Any]) -> str:
    file_status = str(row.get("raw_file_status") or "").strip().lower()
    if file_status == "processing":
        return "processing"
    return {
        "waiting_image": "waiting",
        "blocked": "blocked",
        "wechat": "wechat",
        "failures": "failure",
    }.get(bucket, "normal")


def read_tail_lines(path: Path, max_lines: int = MAX_LOG_LINES) -> list[str]:
    if not path.exists():
        return ["Log ainda nao encontrado."]
    try:
        with path.open("r", encoding="utf-8", errors="replace") as handle:
            return [line.rstrip("\r\n") for line in deque(handle, maxlen=max_lines)]
    except Exception as exc:
        return [f"Nao foi possivel ler o log: {type(exc).__name__}: {exc}"]


def process_status(pid_file: Path) -> tuple[str, bool]:
    if not pid_file.exists():
        return "Daemon parado", False
    try:
        raw = pid_file.read_text(encoding="ascii", errors="ignore").strip().splitlines()[0]
        pid = int(raw)
    except Exception:
        return "PID invalido", False
    try:
        result = subprocess.run(
            ["tasklist", "/FI", f"PID eq {pid}", "/FO", "CSV", "/NH"],
            capture_output=True,
            text=True,
            timeout=5,
            check=False,
        )
        output = (result.stdout or "").strip()
        if output and "No tasks are running" not in output and "INFO:" not in output:
            return f"Daemon rodando | PID {pid}", True
    except Exception:
        try:
            os.kill(pid, 0)
            return f"Daemon rodando | PID {pid}", True
        except Exception:
            pass
    return f"Daemon parado | ultimo PID {pid}", False


def sqlite_connect_ro(db_path: Path) -> sqlite3.Connection:
    conn = sqlite3.connect(str(db_path), timeout=2.0)
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA query_only = ON")
    return conn


def scalar(cur: sqlite3.Cursor, sql: str, params: tuple[Any, ...] = ()) -> int:
    row = cur.execute(sql, params).fetchone()
    return int(row[0]) if row and row[0] is not None else 0


def scalar_text(cur: sqlite3.Cursor, sql: str, params: tuple[Any, ...] = ()) -> str:
    row = cur.execute(sql, params).fetchone()
    return str(row[0] or "").strip() if row else ""


def sql_table_exists(cur: sqlite3.Cursor, table_name: str) -> bool:
    row = cur.execute(
        "SELECT COUNT(*) FROM sqlite_master WHERE type='table' AND name=?",
        (table_name,),
    ).fetchone()
    return bool(row and row[0])


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


def read_ui_force_config_default(base_dir: Path) -> bool:
    cfg_path = base_dir / SINK_CONFIG_FILE
    if not cfg_path.exists():
        return False
    try:
        raw = json.loads(cfg_path.read_text(encoding="utf-8"))
    except Exception:
        return False
    if not isinstance(raw, dict):
        return False
    return parse_boolish(raw.get("ui_force_download_enabled"), default=False)


def read_ui_force_runtime_enabled(base_dir: Path) -> bool:
    default_enabled = read_ui_force_config_default(base_dir)
    db_path = base_dir / "wechat_receipt_state.db"
    if not db_path.exists():
        return default_enabled
    conn: Optional[sqlite3.Connection] = None
    try:
        conn = sqlite3.connect(str(db_path), timeout=2.0)
        row = conn.execute(
            "SELECT value FROM meta WHERE key=? LIMIT 1",
            (UI_FORCE_RUNTIME_META_KEY,),
        ).fetchone()
    except Exception:
        return default_enabled
    finally:
        if conn is not None:
            conn.close()
    if row is None:
        return default_enabled
    return parse_boolish(row[0], default=default_enabled)


def set_ui_force_runtime_enabled(base_dir: Path, enabled: bool) -> tuple[bool, str]:
    db_path = base_dir / "wechat_receipt_state.db"
    if not db_path.exists():
        return False, "Banco nao encontrado para aplicar o modo manual."

    now = time.time()
    conn: Optional[sqlite3.Connection] = None
    try:
        conn = sqlite3.connect(str(db_path), timeout=6.0)
        cur = conn.cursor()
        cur.execute("BEGIN IMMEDIATE")
        cur.execute(
            """
            INSERT INTO meta(key, value, updated_at)
            VALUES(?, ?, ?)
            ON CONFLICT(key) DO UPDATE SET
                value=excluded.value,
                updated_at=excluded.updated_at
            """,
            (UI_FORCE_RUNTIME_META_KEY, "1" if enabled else "0", float(now)),
        )
        if not enabled:
            cur.execute(
                """
                INSERT INTO meta(key, value, updated_at)
                VALUES(?, ?, ?)
                ON CONFLICT(key) DO UPDATE SET
                    value=excluded.value,
                    updated_at=excluded.updated_at
                """,
                (MANUAL_SESSION_META_KEY, str(float(now)), float(now)),
            )

        released_jobs = 0
        requeued_files = 0
        if not enabled:
            has_message_jobs = sql_table_exists(cur, "message_jobs")
            if has_message_jobs:
                released_jobs = int(
                    cur.execute(
                        """
                        UPDATE message_jobs
                        SET state='WAITING_ORIGINAL',
                            batch_id=NULL,
                            next_ui_attempt_at=0,
                            last_seen_at=?,
                            last_ui_result=CASE
                                WHEN last_ui_result IS NULL OR last_ui_result=''
                                THEN 'UI_FORCE_DISABLED_MANUAL_MODE'
                                ELSE last_ui_result
                            END
                        WHERE state IN ('UI_FORCE_PENDING', 'UI_FORCE_RUNNING')
                        """,
                        (float(now),),
                    ).rowcount
                    or 0
                )
            requeued_files = int(
                cur.execute(
                    """
                    UPDATE files
                    SET status=CASE
                            WHEN status='processing' THEN 'processing'
                            ELSE 'retry'
                        END,
                        next_attempt=CASE
                            WHEN next_attempt > ? THEN ?
                            ELSE next_attempt
                        END,
                        last_error='WAITING_ORIGINAL_MEDIA'
                    WHERE status IN ('pending', 'retry', 'processing')
                      AND last_error='WAITING_UI_FORCE_DOWNLOAD'
                    """,
                    (float(now + 3), float(now + 3)),
                ).rowcount
                or 0
            )
        conn.commit()
    except Exception as exc:
        if conn is not None:
            try:
                conn.rollback()
            except Exception:
                pass
        return False, f"Falha ao atualizar modo UI: {type(exc).__name__}: {exc}"
    finally:
        if conn is not None:
            conn.close()

    if enabled:
        return True, "Auto clique no WeChat ativado."
    return True, f"Modo manual ativo (auto clique OFF). Itens liberados: jobs={released_jobs}, fila={requeued_files}."


def clear_queue_backlog(base_dir: Path) -> tuple[bool, str]:
    db_path = base_dir / "wechat_receipt_state.db"
    if not db_path.exists():
        return False, "Banco nao encontrado para limpar a fila."

    cutoff = time.time()
    conn: Optional[sqlite3.Connection] = None
    try:
        conn = sqlite3.connect(str(db_path), timeout=8.0)
        cur = conn.cursor()
        cur.execute("BEGIN IMMEDIATE")

        files_ignored = int(
            cur.execute(
                """
                UPDATE files
                SET status='ignored',
                    processed_at=?,
                    next_attempt=0,
                    last_error=?
                WHERE status IN ('pending', 'retry', 'processing', 'exception', 'failed')
                  AND COALESCE(last_seen, first_seen, mtime, ctime, 0) <= ?
                """,
                (float(cutoff), IGNORE_QUEUE_REASON, float(cutoff)),
            ).rowcount
            or 0
        )

        has_message_jobs = sql_table_exists(cur, "message_jobs")
        message_jobs_cleared = 0
        if has_message_jobs:
            message_jobs_cleared = int(
                cur.execute(
                    """
                    UPDATE message_jobs
                    SET state=?,
                        batch_id=NULL,
                        next_ui_attempt_at=0,
                        last_seen_at=?,
                        ui_force_completed_at=CASE
                            WHEN COALESCE(ui_force_completed_at, 0) > 0 THEN ui_force_completed_at
                            ELSE ?
                        END,
                        last_ui_result=?
                    WHERE state NOT IN ('RESOLVED', 'THUMB_FALLBACK', 'EXCEPTION', 'IGNORED_SESSION_ROLLOVER', 'IGNORED_STALE_MANUAL_SESSION', ?)
                      AND COALESCE(last_seen_at, first_seen_at, create_time, 0) <= ?
                    """,
                    (
                        IGNORED_BY_USER_STATE,
                        float(cutoff),
                        float(cutoff),
                        IGNORE_QUEUE_REASON,
                        IGNORED_BY_USER_STATE,
                        float(cutoff),
                    ),
                ).rowcount
                or 0
            )

        sink_cleared = int(
            cur.execute(
                """
                UPDATE receipts
                SET sheet_status='SINK_SKIPPED_BY_USER_CLEAR_QUEUE',
                    sheet_next_attempt=0,
                    sheet_last_error=?
                WHERE COALESCE(sheet_status, '') IN ('SINK_PENDING', 'SINK_BLOCKED_PRIOR_MSG', 'SINK_RETRY', 'SINK_RUNNING')
                  AND COALESCE(ingested_at, 0) <= ?
                """,
                (IGNORE_QUEUE_REASON, float(cutoff)),
            ).rowcount
            or 0
        )

        cur.execute(
            """
            INSERT INTO meta(key, value, updated_at)
            VALUES(?, ?, ?)
            ON CONFLICT(key) DO UPDATE SET
                value=excluded.value,
                updated_at=excluded.updated_at
            """,
            ("last_manual_queue_clear_at", str(float(cutoff)), float(cutoff)),
        )
        cur.execute(
            """
            INSERT INTO meta(key, value, updated_at)
            VALUES(?, ?, ?)
            ON CONFLICT(key) DO UPDATE SET
                value=excluded.value,
                updated_at=excluded.updated_at
            """,
            (MANUAL_SESSION_META_KEY, str(float(cutoff)), float(cutoff)),
        )
        conn.commit()
    except Exception as exc:
        if conn is not None:
            try:
                conn.rollback()
            except Exception:
                pass
        return False, f"Falha ao limpar fila: {type(exc).__name__}: {exc}"
    finally:
        if conn is not None:
            conn.close()

    return (
        True,
        "Fila antiga descartada. "
        f"Arquivos={files_ignored}, mensagens={message_jobs_cleared}, pendencias_sink={sink_cleared}.",
    )


def ignore_selected_queue_item(base_dir: Path, file_id: str) -> tuple[bool, str]:
    db_path = base_dir / "wechat_receipt_state.db"
    if not db_path.exists():
        return False, "Banco nao encontrado para ignorar o item."

    file_id_value = str(file_id or "").strip()
    if not file_id_value:
        return False, "Item invalido para ignorar."

    now = time.time()
    conn: Optional[sqlite3.Connection] = None
    try:
        conn = sqlite3.connect(str(db_path), timeout=8.0)
        conn.row_factory = sqlite3.Row
        cur = conn.cursor()
        cur.execute("BEGIN IMMEDIATE")

        has_message_jobs = sql_table_exists(cur, "message_jobs")
        has_receipts = sql_table_exists(cur, "receipts")

        row = cur.execute(
            """
            SELECT
                f.file_id,
                f.path,
                COALESCE(
                    NULLIF(f.msg_svr_id, ''),
                    (
                        SELECT mj.msg_svr_id
                        FROM message_jobs mj
                        WHERE mj.expected_image_path = f.path OR mj.thumb_path = f.path
                        ORDER BY mj.last_seen_at DESC
                        LIMIT 1
                    ),
                    ''
                ) AS related_msg_svr_id
            FROM files f
            WHERE f.file_id=?
            LIMIT 1
            """,
            (file_id_value,),
        ).fetchone()
        if row is None:
            return False, "O item selecionado nao foi encontrado na fila."

        msg_svr_id = str(row["related_msg_svr_id"] or "").strip()
        related_paths: list[str] = []
        if has_message_jobs and msg_svr_id:
            msg_row = cur.execute(
                """
                SELECT thumb_path, expected_image_path
                FROM message_jobs
                WHERE msg_svr_id=?
                LIMIT 1
                """,
                (msg_svr_id,),
            ).fetchone()
            if msg_row is not None:
                for key in ("thumb_path", "expected_image_path"):
                    raw_path = str(msg_row[key] or "").strip()
                    if raw_path and raw_path not in related_paths:
                        related_paths.append(raw_path)
        selected_path = str(row["path"] or "").strip()
        if selected_path and selected_path not in related_paths:
            related_paths.append(selected_path)

        file_conditions = ["file_id=?"]
        file_params: list[Any] = [file_id_value]
        if msg_svr_id:
            file_conditions.append("COALESCE(msg_svr_id, '')=?")
            file_params.append(msg_svr_id)
        if related_paths:
            placeholders = ",".join("?" for _ in related_paths)
            file_conditions.append(f"path IN ({placeholders})")
            file_params.extend(related_paths)
        active_statuses_sql = ",".join("?" for _ in ACTIVE_QUEUE_FILE_STATUSES)
        files_ignored = int(
            cur.execute(
                f"""
                UPDATE files
                SET status='ignored',
                    processed_at=?,
                    next_attempt=0,
                    last_error=?
                WHERE ({' OR '.join(file_conditions)})
                  AND status IN ({active_statuses_sql})
                """,
                (float(now), IGNORE_ITEM_REASON, *file_params, *ACTIVE_QUEUE_FILE_STATUSES),
            ).rowcount
            or 0
        )

        message_jobs_ignored = 0
        if has_message_jobs and msg_svr_id:
            message_jobs_ignored = int(
                cur.execute(
                    """
                    UPDATE message_jobs
                    SET state=?,
                        batch_id=NULL,
                        next_ui_attempt_at=0,
                        last_seen_at=?,
                        ui_force_completed_at=CASE
                            WHEN COALESCE(ui_force_completed_at, 0) > 0 THEN ui_force_completed_at
                            ELSE ?
                        END,
                        last_ui_result=?
                    WHERE msg_svr_id=?
                      AND state NOT IN ('RESOLVED', 'THUMB_FALLBACK', 'EXCEPTION', 'IGNORED_SESSION_ROLLOVER', 'IGNORED_STALE_MANUAL_SESSION', ?)
                    """,
                    (
                        IGNORED_BY_USER_STATE,
                        float(now),
                        float(now),
                        IGNORE_ITEM_REASON,
                        msg_svr_id,
                        IGNORED_BY_USER_STATE,
                    ),
                ).rowcount
                or 0
            )

        sink_skipped = 0
        if has_receipts:
            receipt_conditions = ["file_id=?"]
            receipt_params: list[Any] = [file_id_value]
            if msg_svr_id:
                receipt_conditions.append("COALESCE(msg_svr_id, '')=?")
                receipt_params.append(msg_svr_id)
            pending_sink_sql = ",".join("?" for _ in PENDING_RECEIPT_STATUSES)
            sink_skipped = int(
                cur.execute(
                    f"""
                    UPDATE receipts
                    SET sheet_status='SINK_SKIPPED_BY_USER_ITEM',
                        sheet_next_attempt=0,
                        sheet_last_error=?
                    WHERE ({' OR '.join(receipt_conditions)})
                      AND COALESCE(sheet_status, '') IN ({pending_sink_sql})
                    """,
                    (IGNORE_ITEM_REASON, *receipt_params, *PENDING_RECEIPT_STATUSES),
                ).rowcount
                or 0
            )

        blocker_keys: list[str] = []
        if msg_svr_id:
            blocker_keys.extend(
                [
                    f"WAITING_PRIOR_MESSAGE_ORDER:{msg_svr_id}",
                    f"WAITING_SESSION_PRIOR_MESSAGE_ORDER:{msg_svr_id}",
                    f"WAITING_PRIOR_SINK_SESSION_MESSAGE:{msg_svr_id}",
                ]
            )
        blocker_keys.append(f"WAITING_PRIOR_SINK_RECEIPT:{msg_svr_id or f'file:{file_id_value}'}")

        released_files = 0
        released_receipts = 0
        if blocker_keys:
            blocker_sql = ",".join("?" for _ in blocker_keys)
            released_files = int(
                cur.execute(
                    f"""
                    UPDATE files
                    SET status='retry',
                        next_attempt=?,
                        last_error=?
                    WHERE file_id<>?
                      AND status IN ('pending', 'retry')
                      AND last_error IN ({blocker_sql})
                    """,
                    (float(now), RELEASED_AFTER_IGNORE_REASON, file_id_value, *blocker_keys),
                ).rowcount
                or 0
            )
            if has_receipts:
                released_receipts = int(
                    cur.execute(
                        f"""
                        UPDATE receipts
                        SET sheet_status='SINK_RETRY',
                            sheet_next_attempt=?,
                            sheet_last_error=?
                        WHERE file_id<>?
                          AND COALESCE(sheet_status, '')='SINK_BLOCKED_PRIOR_MSG'
                          AND sheet_last_error IN ({blocker_sql})
                        """,
                        (float(now), RELEASED_AFTER_IGNORE_REASON, file_id_value, *blocker_keys),
                    ).rowcount
                    or 0
                )

        conn.commit()
    except Exception as exc:
        if conn is not None:
            try:
                conn.rollback()
            except Exception:
                pass
        return False, f"Falha ao ignorar item: {type(exc).__name__}: {exc}"
    finally:
        if conn is not None:
            conn.close()

    return (
        True,
        "Mensagem ignorada com seguranca. "
        f"Arquivos relacionados={files_ignored}, mensagem={message_jobs_ignored}, "
        f"sink pulado={sink_skipped}, liberados={released_files + released_receipts}.",
    )


def stop_daemon_processing(base_dir: Path) -> tuple[bool, str]:
    pid_path = base_dir / "wechat_receipt.pid"
    if not pid_path.exists():
        return True, "Daemon ja estava parado."

    try:
        raw = pid_path.read_text(encoding="ascii", errors="ignore").strip().splitlines()[0]
        pid = int(raw)
    except Exception:
        try:
            pid_path.unlink(missing_ok=True)
        except Exception:
            pass
        return True, "PID invalido removido. Daemon considerado parado."

    try:
        result = subprocess.run(
            ["taskkill", "/PID", str(pid), "/T", "/F"],
            capture_output=True,
            text=True,
            timeout=10,
            check=False,
        )
    except Exception as exc:
        return False, f"Falha ao executar taskkill: {type(exc).__name__}: {exc}"
    output = f"{result.stdout}\n{result.stderr}".strip().lower()
    not_found_tokens = (
        "no running instance",
        "not found",
        "nao foi",
        "cannot find",
        "not valid",
    )
    stopped = result.returncode == 0 or any(token in output for token in not_found_tokens)
    if stopped:
        try:
            pid_path.unlink(missing_ok=True)
        except Exception:
            pass
        return True, f"Processamento parado (PID {pid})."
    return False, f"Falha ao parar daemon PID {pid}: {short_text(output, limit=180)}"


def restart_daemon_processing(base_dir: Path) -> tuple[bool, str]:
    stop_ok, stop_message = stop_daemon_processing(base_dir)
    if not stop_ok:
        return False, f"Nao foi possivel parar antes de reiniciar: {stop_message}"
    start_script = base_dir / "INICIAR_WECHAT_OCR.ps1"
    if not start_script.exists():
        return False, f"Script de inicio nao encontrado: {start_script}"

    time.sleep(1.0)
    try:
        result = subprocess.run(
            ["powershell", "-NoProfile", "-ExecutionPolicy", "Bypass", "-File", str(start_script)],
            capture_output=True,
            text=True,
            timeout=120,
            check=False,
            cwd=str(base_dir),
        )
    except Exception as exc:
        return False, f"Falha ao iniciar novamente: {type(exc).__name__}: {exc}"

    daemon_status, daemon_running = process_status(base_dir / "wechat_receipt.pid")
    stdout = short_text(result.stdout or "", limit=160)
    stderr = short_text(result.stderr or "", limit=160)
    details = " | ".join(part for part in [stdout, stderr] if part and part != "-")

    if daemon_running:
        status_msg = f"Processamento reiniciado. {daemon_status}."
        if details:
            return True, f"{status_msg} Saida: {details}"
        return True, status_msg

    if details:
        return False, f"Falha ao reiniciar. {details}"
    return False, f"Falha ao reiniciar. {stop_message}"


def load_snapshot(base_dir: Path) -> DashboardSnapshot:
    db_path = base_dir / "wechat_receipt_state.db"
    log_path = base_dir / "wechat_receipt.out.log"
    pid_path = base_dir / "wechat_receipt.pid"
    ui_force_runtime_enabled = read_ui_force_runtime_enabled(base_dir)

    daemon_status, daemon_running = process_status(pid_path)
    snapshot = DashboardSnapshot(
        daemon_status=daemon_status,
        daemon_running=daemon_running,
        log_lines=read_tail_lines(log_path),
        ui_force_runtime_enabled=ui_force_runtime_enabled,
    )

    if not db_path.exists():
        snapshot.error = f"Banco nao encontrado: {db_path}"
        return snapshot

    recent_floor = time.time() - 24 * 3600
    conn: Optional[sqlite3.Connection] = None
    try:
        conn = sqlite_connect_ro(db_path)
        cur = conn.cursor()
        if cur is not None:
            message_jobs_exists = sql_table_exists(cur, "message_jobs")
            receipts_exists = sql_table_exists(cur, "receipts")

            blocked_files = scalar(
                cur,
                """
                SELECT COUNT(*)
                FROM files
                WHERE status IN ('pending', 'retry', 'processing')
                  AND (
                        last_error LIKE 'WAITING_PRIOR_MESSAGE_ORDER:%'
                     OR last_error LIKE 'WAITING_SESSION_PRIOR_MESSAGE_ORDER:%'
                  )
                """,
            )
            blocked_receipts = scalar(
                cur,
                "SELECT COUNT(*) FROM receipts WHERE COALESCE(sheet_status, '')='SINK_BLOCKED_PRIOR_MSG'",
            ) if receipts_exists else 0
            attention_count = scalar(cur, "SELECT COUNT(*) FROM files WHERE status IN ('exception', 'failed')")
            if receipts_exists:
                attention_count += scalar(cur, "SELECT COUNT(*) FROM receipts WHERE COALESCE(sheet_status, '')='SINK_RETRY'")

            snapshot.metrics = {
                "Na fila": str(scalar(cur, "SELECT COUNT(*) FROM files WHERE status IN ('pending','retry','processing')")),
                "Esperando imagem": str(
                    scalar(
                        cur,
                        """
                        SELECT COUNT(*)
                        FROM files
                        WHERE status IN ('pending', 'retry', 'processing')
                          AND last_error IN ('WAITING_ORIGINAL_MEDIA', 'MANUAL_WAIT_ORIGINAL', 'WAITING_TEMP_CONTEXT', 'WAITING_TEMP_DB_MATCH')
                        """,
                    )
                ),
                "Bloqueadas por ordem": str(blocked_files + blocked_receipts),
                "Aguardando WeChat": str(
                    scalar(cur, "SELECT COUNT(*) FROM message_jobs WHERE state='UI_FORCE_PENDING'") if message_jobs_exists else 0
                ),
                "WeChat em acao": str(
                    scalar(cur, "SELECT COUNT(*) FROM message_jobs WHERE state='UI_FORCE_RUNNING'") if message_jobs_exists else 0
                ),
                "Processando": str(scalar(cur, "SELECT COUNT(*) FROM files WHERE status='processing'")),
                "Precisam atencao": str(attention_count),
                "Recebidos 24h": str(
                    scalar(cur, "SELECT COUNT(*) FROM receipts WHERE ingested_at >= ?", (recent_floor,))
                    if receipts_exists
                    else 0
                ),
            }

            if message_jobs_exists:
                queue_sql = """
                    SELECT
                        f.file_id,
                        f.path,
                        f.source_kind,
                        f.status,
                        f.attempts,
                        f.first_seen,
                        f.next_attempt,
                        f.last_error,
                        f.msg_svr_id,
                        COALESCE(
                            NULLIF(mj.talker_display, ''),
                            mj.talker,
                            (
                                SELECT COALESCE(NULLIF(mjx.talker_display, ''), mjx.talker)
                                FROM message_jobs mjx
                                WHERE mjx.thumb_path = f.path OR mjx.expected_image_path = f.path
                                ORDER BY mjx.last_seen_at DESC
                                LIMIT 1
                            ),
                            f.talker,
                            '-'
                        ) AS talker_name,
                        COALESCE(
                            NULLIF(mj.state, ''),
                            (
                                SELECT mjx.state
                                FROM message_jobs mjx
                                WHERE mjx.thumb_path = f.path OR mjx.expected_image_path = f.path
                                ORDER BY mjx.last_seen_at DESC
                                LIMIT 1
                            ),
                            ''
                        ) AS message_state,
                        COALESCE(
                            NULLIF(mj.last_ui_result, ''),
                            (
                                SELECT COALESCE(mjx.last_ui_result, '')
                                FROM message_jobs mjx
                                WHERE mjx.thumb_path = f.path OR mjx.expected_image_path = f.path
                                ORDER BY mjx.last_seen_at DESC
                                LIMIT 1
                            ),
                            ''
                        ) AS last_ui_result,
                        COALESCE(
                            NULLIF(mj.msg_svr_id, ''),
                            (
                                SELECT mjx.msg_svr_id
                                FROM message_jobs mjx
                                WHERE mjx.thumb_path = f.path OR mjx.expected_image_path = f.path
                                ORDER BY mjx.last_seen_at DESC
                                LIMIT 1
                            ),
                            f.msg_svr_id,
                            ''
                        ) AS related_msg_svr_id
                    FROM files f
                    LEFT JOIN message_jobs mj ON mj.msg_svr_id = f.msg_svr_id
                    WHERE f.status IN ('pending', 'retry', 'processing', 'exception', 'failed')
                    ORDER BY
                        CASE f.status
                            WHEN 'processing' THEN 0
                            WHEN 'exception' THEN 1
                            WHEN 'failed' THEN 1
                            ELSE 2
                        END ASC,
                        f.first_seen ASC,
                        f.mtime ASC
                    LIMIT ?
                """
            else:
                queue_sql = """
                    SELECT
                        f.file_id,
                        f.path,
                        f.source_kind,
                        f.status,
                        f.attempts,
                        f.first_seen,
                        f.next_attempt,
                        f.last_error,
                        f.msg_svr_id,
                        COALESCE(f.talker, '-') AS talker_name,
                        '' AS message_state,
                        '' AS last_ui_result,
                        COALESCE(f.msg_svr_id, '') AS related_msg_svr_id
                    FROM files f
                    WHERE f.status IN ('pending', 'retry', 'processing', 'exception', 'failed')
                    ORDER BY
                        CASE f.status
                            WHEN 'processing' THEN 0
                            WHEN 'exception' THEN 1
                            WHEN 'failed' THEN 1
                            ELSE 2
                        END ASC,
                        f.first_seen ASC,
                        f.mtime ASC
                    LIMIT ?
                """

            for row in cur.execute(queue_sql, (MAX_QUEUE_ROWS,)).fetchall():
                raw_wait = str(row["last_error"] or "").strip()
                raw_message_state = str(row["message_state"] or "").strip()
                payload = {
                    "file_id": str(row["file_id"]),
                    "msg_svr_id": str(row["related_msg_svr_id"] or "").strip(),
                    "grupo": short_text(row["talker_name"], limit=30),
                    "arquivo": compact_path(row["path"], limit=54),
                    "arquivo_estado": file_status_label(row["status"]),
                    "mensagem_estado": message_state_label(raw_message_state),
                    "motivo": wait_reason_label(raw_wait),
                    "tentativas": str(int(row["attempts"] or 0)),
                    "idade": fmt_age(row["first_seen"]),
                    "proximo": fmt_dt(row["next_attempt"]),
                    "tipo": source_kind_label(row["source_kind"]),
                    "last_ui_result": short_text(row["last_ui_result"], limit=58),
                    "open_path": str(row["path"] or ""),
                    "full_path": str(row["path"] or ""),
                    "technical_code": (
                        f"file={str(row['status'] or '').strip()} | "
                        f"msg={raw_message_state or '-'} | "
                        f"wait={raw_wait or '-'}"
                    ),
                    "raw_wait_code": raw_wait,
                    "raw_message_state": raw_message_state,
                    "raw_file_status": str(row["status"] or "").strip(),
                }
                payload["queue_bucket"] = queue_filter_bucket(payload)
                payload["row_tag"] = row_tag_for_bucket(str(payload["queue_bucket"]), payload)
                snapshot.queue_rows.append(payload)

            if message_jobs_exists:
                message_sql = """
                    SELECT
                        msg_svr_id,
                        COALESCE(NULLIF(talker_display, ''), talker) AS talker_name,
                        create_time,
                        state,
                        ui_force_attempts,
                        last_ui_result,
                        expected_image_path,
                        thumb_path
                    FROM message_jobs
                    WHERE state NOT IN ('RESOLVED', 'THUMB_FALLBACK', 'EXCEPTION', 'IGNORED_SESSION_ROLLOVER', 'IGNORED_STALE_MANUAL_SESSION', ?)
                    ORDER BY create_time ASC, msg_svr_id ASC
                    LIMIT ?
                """
                for row in cur.execute(message_sql, (IGNORED_BY_USER_STATE, MAX_MESSAGE_ROWS)).fetchall():
                    open_path = str(row["expected_image_path"] or row["thumb_path"] or "")
                    snapshot.message_rows.append(
                        {
                            "envio": fmt_dt(row["create_time"]),
                            "grupo": short_text(row["talker_name"], limit=34),
                            "estado": message_state_label(row["state"]),
                            "ui_try": str(int(row["ui_force_attempts"] or 0)),
                            "ultimo_ui": short_text(row["last_ui_result"], limit=42),
                            "arquivo": compact_path(open_path, limit=54),
                            "open_path": open_path,
                            "msg_svr_id": str(row["msg_svr_id"] or "").strip(),
                            "raw_state": str(row["state"] or "").strip(),
                        }
                    )

                snapshot.last_ui_result = scalar_text(cur, "SELECT value FROM meta WHERE key='last_ui_result' LIMIT 1") or "-"
                snapshot.last_ui_talker = scalar_text(cur, "SELECT value FROM meta WHERE key='last_ui_talker' LIMIT 1") or "-"
                snapshot.last_exception = scalar_text(cur, "SELECT value FROM meta WHERE key='last_exception_reason' LIMIT 1") or "-"
                snapshot.last_resolution = scalar_text(cur, "SELECT value FROM meta WHERE key='last_resolution_source' LIMIT 1") or "-"
                snapshot.last_verification = scalar_text(cur, "SELECT value FROM meta WHERE key='last_verification_status' LIMIT 1") or "-"

            if receipts_exists:
                receipt_sql = """
                    SELECT
                        ingested_at,
                        COALESCE(NULLIF(client, ''), talker, '-') AS owner_name,
                        COALESCE(bank, '-') AS bank,
                        COALESCE(amount, '') AS amount,
                        COALESCE(txn_date, '-') AS txn_date,
                        COALESCE(txn_time, '-') AS txn_time,
                        COALESCE(verification_status, '-') AS verification_status,
                        COALESCE(resolution_source, '-') AS resolution_source
                    FROM receipts
                    ORDER BY ingested_at DESC
                    LIMIT ?
                """
                for row in cur.execute(receipt_sql, (MAX_RECEIPT_ROWS,)).fetchall():
                    amount_text = "-" if row["amount"] in (None, "") else str(row["amount"])
                    snapshot.receipt_rows.append(
                        {
                            "processado": fmt_dt(row["ingested_at"]),
                            "cliente": short_text(row["owner_name"], limit=32),
                            "banco": short_text(row["bank"], limit=18),
                            "valor": amount_text,
                            "comprovante": f"{row['txn_date']} {row['txn_time']}".strip(),
                            "verificacao": short_text(row["verification_status"], limit=20),
                            "origem": short_text(row["resolution_source"], limit=20),
                        }
                    )
    except Exception as exc:
        snapshot.error = f"{type(exc).__name__}: {exc}"
    finally:
        if conn is not None:
            conn.close()

    return snapshot


class MetricCard(tk.Frame):
    def __init__(self, master: tk.Misc, title: str, accent: str) -> None:
        super().__init__(
            master,
            bg=PALETTE["surface"],
            highlightbackground=PALETTE["border"],
            highlightthickness=1,
            bd=0,
            padx=14,
            pady=12,
        )
        tk.Frame(self, bg=accent, height=4).pack(fill="x", side="top")
        self.title_label = tk.Label(
            self,
            text=title,
            bg=PALETTE["surface"],
            fg=PALETTE["muted"],
            font=("Segoe UI", 9, "bold"),
            anchor="w",
            pady=6,
        )
        self.title_label.pack(fill="x")
        self.value_label = tk.Label(
            self,
            text="-",
            bg=PALETTE["surface"],
            fg=PALETTE["text"],
            font=("Segoe UI", 22, "bold"),
            anchor="w",
        )
        self.value_label.pack(fill="x", pady=(4, 0))

    def set_value(self, value: str) -> None:
        self.value_label.configure(text=str(value))


class DashboardApp(tk.Tk):
    def __init__(self, base_dir: Path) -> None:
        super().__init__()
        self.base_dir = base_dir
        self.db_path = base_dir / "wechat_receipt_state.db"
        self.log_path = base_dir / "wechat_receipt.out.log"
        self._job: Optional[str] = None
        self._snapshot = DashboardSnapshot(daemon_status="-", daemon_running=False)
        self._queue_rows: list[dict[str, Any]] = []
        self._queue_item_rows: dict[str, dict[str, Any]] = {}
        self._queue_item_paths: dict[str, str] = {}
        self._message_item_paths: dict[str, str] = {}
        self._ui_force_runtime_enabled = read_ui_force_runtime_enabled(base_dir)
        self.queue_search_var = tk.StringVar(value="")
        self.queue_filter_var = tk.StringVar(value="all")

        self.title(APP_TITLE)
        self.geometry("1460x940")
        self.minsize(1220, 780)
        self.configure(bg=PALETTE["bg"])

        self.style = ttk.Style(self)
        self._configure_theme()

        self._build_header()
        self._build_cards()
        self._build_notebook()
        self._build_footer()
        self.queue_search_var.trace_add("write", self._on_queue_search_change)

        self.after(120, self._present_window)
        self.refresh_now()

    def _configure_theme(self) -> None:
        self.style.theme_use("clam")
        self.style.configure(
            "Treeview",
            rowheight=27,
            font=("Segoe UI", 9),
            background=PALETTE["surface"],
            fieldbackground=PALETTE["surface"],
        )
        self.style.configure(
            "Treeview.Heading",
            font=("Segoe UI", 9, "bold"),
            background="#dce7f3",
            foreground=PALETTE["text"],
            relief="flat",
        )
        self.style.map(
            "Treeview",
            background=[("selected", "#cfe1ff")],
            foreground=[("selected", PALETTE["text"])],
        )
        self.style.configure("TNotebook", background=PALETTE["bg"], borderwidth=0)
        self.style.configure("TNotebook.Tab", padding=(14, 9), font=("Segoe UI", 9, "bold"), background="#dce7f3")

    def _present_window(self) -> None:
        try:
            self.update_idletasks()
        except Exception:
            pass
        try:
            self.deiconify()
        except Exception:
            pass
        try:
            screen_w = max(1280, int(self.winfo_screenwidth()))
            screen_h = max(820, int(self.winfo_screenheight()))
            width = min(1460, screen_w - 80)
            height = min(940, screen_h - 80)
            x = max(20, (screen_w - width) // 2)
            y = max(20, (screen_h - height) // 2)
            self.geometry(f"{width}x{height}+{x}+{y}")
        except Exception:
            pass
        try:
            self.lift()
            self.attributes("-topmost", True)
            self.after(700, lambda: self.attributes("-topmost", False))
        except Exception:
            pass
        try:
            self.focus_force()
        except Exception:
            pass

    def _build_header(self) -> None:
        header = tk.Frame(self, bg=PALETTE["header"], padx=20, pady=18)
        header.pack(fill="x", padx=14, pady=(14, 8))
        header.grid_columnconfigure(0, weight=3)
        header.grid_columnconfigure(1, weight=2)
        header.grid_columnconfigure(2, weight=2)

        left = tk.Frame(header, bg=PALETTE["header"])
        left.grid(row=0, column=0, sticky="nsew", padx=(0, 18))
        tk.Label(
            left,
            text=APP_TITLE,
            bg=PALETTE["header"],
            fg="#f8fbff",
            font=("Segoe UI", 21, "bold"),
        ).pack(anchor="w")
        tk.Label(
            left,
            text="Fila mais clara, acoes mais seguras e leitura rapida sem precisar abrir o terminal.",
            bg=PALETTE["header"],
            fg=PALETTE["header_muted"],
            font=("Segoe UI", 10),
        ).pack(anchor="w", pady=(4, 12))

        chips = tk.Frame(left, bg=PALETTE["header"])
        chips.pack(anchor="w")
        self.status_label = tk.Label(
            chips,
            text="Daemon: -",
            bg="#19314f",
            fg="#f8fbff",
            font=("Segoe UI", 10, "bold"),
            padx=12,
            pady=8,
        )
        self.status_label.pack(side="left")
        self.ui_toggle_button = tk.Button(
            chips,
            text="Auto clique: -",
            command=self.toggle_ui_force_runtime,
            relief="flat",
            padx=12,
            pady=8,
            bd=0,
            font=("Segoe UI", 10, "bold"),
            cursor="hand2",
        )
        self.ui_toggle_button.pack(side="left", padx=(8, 0))
        self._render_ui_toggle_button(self._ui_force_runtime_enabled)

        safe_actions = tk.Frame(header, bg=PALETTE["header"])
        safe_actions.grid(row=0, column=1, sticky="nsew", padx=(0, 12))
        tk.Label(safe_actions, text="Acoes rapidas", bg=PALETTE["header"], fg="#f8fbff", font=("Segoe UI", 10, "bold")).pack(anchor="w")
        tk.Label(
            safe_actions,
            text="Atualize a tela ou abra o log quando quiser conferir os detalhes tecnicos.",
            bg=PALETTE["header"],
            fg=PALETTE["header_muted"],
            font=("Segoe UI", 9),
            justify="left",
            wraplength=260,
        ).pack(anchor="w", pady=(4, 10))
        safe_row = tk.Frame(safe_actions, bg=PALETTE["header"])
        safe_row.pack(anchor="w")
        self._make_action_button(safe_row, "Atualizar agora", self.refresh_now, "#f59e0b", "#111827").pack(side="left")
        self._make_action_button(safe_row, "Abrir log", lambda: self._open_path(self.log_path), "#2563eb", "#ffffff").pack(side="left", padx=(8, 0))

        risk_actions = tk.Frame(header, bg=PALETTE["danger_bg"], padx=14, pady=12)
        risk_actions.grid(row=0, column=2, sticky="nsew")
        tk.Label(risk_actions, text="Acoes de risco", bg=PALETTE["danger_bg"], fg="#fff7f7", font=("Segoe UI", 10, "bold")).pack(anchor="w")
        tk.Label(
            risk_actions,
            text="Use so quando realmente quiser limpar a fila antiga ou reiniciar o processamento.",
            bg=PALETTE["danger_bg"],
            fg="#fecaca",
            font=("Segoe UI", 9),
            justify="left",
            wraplength=260,
        ).pack(anchor="w", pady=(4, 10))
        danger_row = tk.Frame(risk_actions, bg=PALETTE["danger_bg"])
        danger_row.pack(anchor="w")
        self._make_action_button(danger_row, "Ignorar fila antiga", self.clear_queue_now, "#f59e0b", "#111827").pack(side="left")
        self._make_action_button(danger_row, "Parar", self.stop_processing_now, PALETTE["danger_alt"], "#ffffff").pack(side="left", padx=(8, 0))
        self._make_action_button(danger_row, "Reiniciar", self.restart_processing_now, "#166534", "#ffffff").pack(side="left", padx=(8, 0))

    def _make_action_button(self, master: tk.Misc, text: str, command: Any, bg: str, fg: str) -> tk.Button:
        return tk.Button(
            master,
            text=text,
            command=command,
            bg=bg,
            fg=fg,
            activebackground=bg,
            activeforeground=fg,
            disabledforeground=fg,
            relief="flat",
            bd=0,
            padx=12,
            pady=8,
            font=("Segoe UI", 9, "bold"),
            cursor="hand2",
        )

    def _build_cards(self) -> None:
        wrap = tk.Frame(self, bg=PALETTE["bg"], padx=14, pady=4)
        wrap.pack(fill="x")
        self.metric_cards: dict[str, MetricCard] = {}
        for col, (title, accent) in enumerate(METRIC_SPECS):
            wrap.grid_columnconfigure(col, weight=1)
            card = MetricCard(wrap, title, accent)
            card.grid(row=0, column=col, sticky="nsew", padx=5, pady=6)
            self.metric_cards[title] = card

    def _build_notebook(self) -> None:
        self.notebook = ttk.Notebook(self)
        self.notebook.pack(fill="both", expand=True, padx=14, pady=(4, 12))
        self._build_queue_tab()
        self._build_message_tab()
        self._build_receipts_tab()
        self._build_log_tab()

    def _build_queue_tab(self) -> None:
        tab = tk.Frame(self.notebook, bg=PALETTE["bg"])
        self.notebook.add(tab, text="Fila atual")
        tab.grid_rowconfigure(1, weight=1)
        tab.grid_columnconfigure(0, weight=1)

        controls = tk.Frame(tab, bg=PALETTE["surface"], padx=14, pady=12, highlightbackground=PALETTE["border"], highlightthickness=1)
        controls.grid(row=0, column=0, sticky="ew", pady=(0, 10))
        controls.grid_columnconfigure(0, weight=1)
        controls.grid_columnconfigure(1, weight=1)

        left = tk.Frame(controls, bg=PALETTE["surface"])
        left.grid(row=0, column=0, sticky="w")
        tk.Label(left, text="Buscar na fila", bg=PALETTE["surface"], fg=PALETTE["muted"], font=("Segoe UI", 9, "bold")).pack(anchor="w")
        search_row = tk.Frame(left, bg=PALETTE["surface"])
        search_row.pack(anchor="w", pady=(6, 0))
        self.search_entry = tk.Entry(
            search_row,
            textvariable=self.queue_search_var,
            relief="flat",
            bd=0,
            highlightthickness=1,
            highlightbackground=PALETTE["border"],
            highlightcolor="#93c5fd",
            bg=PALETTE["surface_alt"],
            fg=PALETTE["text"],
            insertbackground=PALETTE["text"],
            width=34,
            font=("Segoe UI", 10),
        )
        self.search_entry.pack(side="left", ipady=6)

        self.filter_buttons: dict[str, tk.Button] = {}
        filter_wrap = tk.Frame(left, bg=PALETTE["surface"])
        filter_wrap.pack(anchor="w", pady=(10, 0))
        for key, label in QUEUE_FILTERS:
            button = tk.Button(
                filter_wrap,
                text=label,
                command=lambda value=key: self._set_queue_filter(value),
                relief="flat",
                bd=0,
                padx=10,
                pady=6,
                font=("Segoe UI", 9, "bold"),
                cursor="hand2",
            )
            button.pack(side="left", padx=(0, 8))
            self.filter_buttons[key] = button
        self._render_filter_buttons()

        right = tk.Frame(controls, bg=PALETTE["surface"])
        right.grid(row=0, column=1, sticky="e")
        self.queue_selection_label = tk.Label(
            right,
            text="Nenhum item selecionado",
            bg=PALETTE["surface"],
            fg=PALETTE["muted"],
            font=("Segoe UI", 9),
        )
        self.queue_selection_label.pack(anchor="e")
        buttons = tk.Frame(right, bg=PALETTE["surface"])
        buttons.pack(anchor="e", pady=(8, 0))
        self.open_selection_button = self._make_action_button(buttons, "Abrir pasta", self._open_selected_queue_item, "#1d4ed8", "#ffffff")
        self.open_selection_button.pack(side="left")
        self.show_selection_button = self._make_action_button(buttons, "Ver detalhes", self._show_selected_details, "#0f766e", "#ffffff")
        self.show_selection_button.pack(side="left", padx=(8, 0))
        self.ignore_selection_button = self._make_action_button(buttons, "Ignorar selecionado", self.ignore_selected_queue_item_now, "#b42318", "#ffffff")
        self.ignore_selection_button.pack(side="left", padx=(8, 0))

        content = tk.Frame(tab, bg=PALETTE["bg"])
        content.grid(row=1, column=0, sticky="nsew")
        content.grid_rowconfigure(0, weight=1)
        content.grid_columnconfigure(0, weight=3)
        content.grid_columnconfigure(1, weight=2)

        table_wrap = tk.Frame(content, bg=PALETTE["surface"], highlightbackground=PALETTE["border"], highlightthickness=1)
        table_wrap.grid(row=0, column=0, sticky="nsew", padx=(0, 10))
        table_wrap.grid_rowconfigure(1, weight=1)
        table_wrap.grid_columnconfigure(0, weight=1)

        top_table_bar = tk.Frame(table_wrap, bg=PALETTE["surface"], padx=12, pady=10)
        top_table_bar.grid(row=0, column=0, sticky="ew", columnspan=2)
        self.queue_count_label = tk.Label(
            top_table_bar,
            text="Fila carregando...",
            bg=PALETTE["surface"],
            fg=PALETTE["muted"],
            font=("Segoe UI", 9, "bold"),
        )
        self.queue_count_label.pack(anchor="w")

        self.queue_tree = ttk.Treeview(
            table_wrap,
            columns=("grupo", "arquivo", "arquivo_estado", "mensagem_estado", "motivo", "tentativas", "proximo"),
            show="headings",
        )
        headings = {
            "grupo": ("Grupo", 230),
            "arquivo": ("Arquivo", 210),
            "arquivo_estado": ("Arquivo", 150),
            "mensagem_estado": ("Mensagem", 170),
            "motivo": ("Motivo atual", 240),
            "tentativas": ("Tent.", 65),
            "proximo": ("Proxima", 135),
        }
        for key, (label, width) in headings.items():
            self.queue_tree.heading(key, text=label)
            self.queue_tree.column(key, width=width, anchor="w")
        self.queue_tree.tag_configure("normal", background=PALETTE["surface"])
        self.queue_tree.tag_configure("processing", background="#ecfdf3")
        self.queue_tree.tag_configure("waiting", background="#fff7ed")
        self.queue_tree.tag_configure("blocked", background="#fff1f2")
        self.queue_tree.tag_configure("wechat", background="#eff6ff")
        self.queue_tree.tag_configure("failure", background="#fef2f2")

        y_scroll = ttk.Scrollbar(table_wrap, orient="vertical", command=self.queue_tree.yview)
        x_scroll = ttk.Scrollbar(table_wrap, orient="horizontal", command=self.queue_tree.xview)
        self.queue_tree.configure(yscrollcommand=y_scroll.set, xscrollcommand=x_scroll.set)
        self.queue_tree.grid(row=1, column=0, sticky="nsew")
        y_scroll.grid(row=1, column=1, sticky="ns")
        x_scroll.grid(row=2, column=0, sticky="ew")
        self.queue_tree.bind("<<TreeviewSelect>>", self._on_queue_selection_change)
        self.queue_tree.bind("<Double-1>", self._open_selected_queue_item)
        self.queue_tree.bind("<Button-3>", self._open_queue_context_menu)

        self.queue_menu = tk.Menu(self, tearoff=0)
        self.queue_menu.add_command(label="Abrir pasta", command=self._open_selected_queue_item)
        self.queue_menu.add_command(label="Ver detalhes", command=self._show_selected_details)
        self.queue_menu.add_separator()
        self.queue_menu.add_command(label="Ignorar selecionado", command=self.ignore_selected_queue_item_now)

        details_wrap = tk.Frame(content, bg=PALETTE["bg"])
        details_wrap.grid(row=0, column=1, sticky="nsew")
        details_wrap.grid_columnconfigure(0, weight=1)
        details_wrap.grid_rowconfigure(1, weight=1)

        details_card = tk.Frame(details_wrap, bg=PALETTE["surface"], padx=14, pady=14, highlightbackground=PALETTE["border"], highlightthickness=1)
        details_card.grid(row=0, column=0, sticky="nsew")
        tk.Label(details_card, text="Detalhes da selecao", bg=PALETTE["surface"], fg=PALETTE["text"], font=("Segoe UI", 11, "bold")).pack(anchor="w")
        tk.Label(
            details_card,
            text="Aqui voce entende o que esta acontecendo com o item escolhido sem precisar decorar os codigos tecnicos.",
            bg=PALETTE["surface"],
            fg=PALETTE["muted"],
            font=("Segoe UI", 9),
            justify="left",
            wraplength=360,
        ).pack(anchor="w", pady=(4, 12))

        self.detail_vars: dict[str, tk.StringVar] = {}
        for key, label in (
            ("grupo", "Grupo"),
            ("arquivo", "Arquivo"),
            ("tipo", "Tipo"),
            ("arquivo_estado", "Status do arquivo"),
            ("mensagem_estado", "Status da mensagem"),
            ("motivo", "Motivo atual"),
            ("tentativas", "Tentativas"),
            ("proximo", "Proxima tentativa"),
            ("idade", "Idade"),
            ("last_ui_result", "Ultimo retorno UI"),
        ):
            self._build_detail_row(details_card, key, label)

        tk.Label(details_card, text="Caminho completo", bg=PALETTE["surface"], fg=PALETTE["muted"], font=("Segoe UI", 9, "bold")).pack(anchor="w", pady=(12, 2))
        self.detail_path_label = tk.Label(
            details_card,
            text="-",
            bg=PALETTE["surface_alt"],
            fg=PALETTE["text"],
            justify="left",
            anchor="w",
            wraplength=360,
            padx=10,
            pady=8,
            highlightbackground=PALETTE["border"],
            highlightthickness=1,
        )
        self.detail_path_label.pack(fill="x")

        tk.Label(details_card, text="Codigo tecnico", bg=PALETTE["surface"], fg=PALETTE["muted"], font=("Segoe UI", 9, "bold")).pack(anchor="w", pady=(12, 2))
        self.detail_code_label = tk.Label(
            details_card,
            text="-",
            bg=PALETTE["surface_alt"],
            fg=PALETTE["text"],
            justify="left",
            anchor="w",
            wraplength=360,
            padx=10,
            pady=8,
            highlightbackground=PALETTE["border"],
            highlightthickness=1,
        )
        self.detail_code_label.pack(fill="x")

        help_card = tk.Frame(details_wrap, bg=PALETTE["surface"], padx=14, pady=14, highlightbackground=PALETTE["border"], highlightthickness=1)
        help_card.grid(row=1, column=0, sticky="nsew", pady=(10, 0))
        tk.Label(help_card, text="Entender status", bg=PALETTE["surface"], fg=PALETTE["text"], font=("Segoe UI", 11, "bold")).pack(anchor="w")
        for title, description in STATUS_HELP_ROWS:
            row = tk.Frame(help_card, bg=PALETTE["surface"])
            row.pack(fill="x", pady=(10, 0))
            tk.Label(row, text=title, bg=PALETTE["surface"], fg=PALETTE["text"], font=("Segoe UI", 9, "bold")).pack(anchor="w")
            tk.Label(
                row,
                text=description,
                bg=PALETTE["surface"],
                fg=PALETTE["muted"],
                font=("Segoe UI", 9),
                justify="left",
                wraplength=360,
            ).pack(anchor="w", pady=(2, 0))

        self._set_detail_row_values(None)

    def _build_detail_row(self, master: tk.Misc, key: str, label: str) -> None:
        row = tk.Frame(master, bg=PALETTE["surface"])
        row.pack(fill="x", pady=(0, 8))
        tk.Label(row, text=label, bg=PALETTE["surface"], fg=PALETTE["muted"], font=("Segoe UI", 9, "bold")).pack(anchor="w")
        var = tk.StringVar(value="-")
        tk.Label(
            row,
            textvariable=var,
            bg=PALETTE["surface"],
            fg=PALETTE["text"],
            font=("Segoe UI", 10),
            anchor="w",
            justify="left",
            wraplength=360,
        ).pack(fill="x")
        self.detail_vars[key] = var

    def _build_message_tab(self) -> None:
        tab = tk.Frame(self.notebook, bg=PALETTE["surface"])
        self.notebook.add(tab, text="Mensagens ativas")
        tab.grid_rowconfigure(1, weight=1)
        tab.grid_columnconfigure(0, weight=1)
        tk.Label(
            tab,
            text="Mostra somente mensagens ainda em andamento. Estados antigos ou ignorados ficam escondidos daqui.",
            bg=PALETTE["surface"],
            fg=PALETTE["muted"],
            font=("Segoe UI", 9),
            anchor="w",
            padx=12,
            pady=10,
        ).grid(row=0, column=0, sticky="ew")
        self.message_tree = self._build_tree_area(
            tab,
            row=1,
            columns=(
                ("envio", "Envio", 145),
                ("grupo", "Grupo", 270),
                ("estado", "Estado", 180),
                ("ui_try", "UI", 60),
                ("ultimo_ui", "Ultimo retorno UI", 280),
                ("arquivo", "Arquivo", 280),
            ),
        )
        self.message_tree.bind("<Double-1>", self._open_selected_message_item)

    def _build_receipts_tab(self) -> None:
        tab = tk.Frame(self.notebook, bg=PALETTE["surface"])
        self.notebook.add(tab, text="Ultimos processados")
        tab.grid_rowconfigure(0, weight=1)
        tab.grid_columnconfigure(0, weight=1)
        self.receipt_tree = self._build_tree_area(
            tab,
            row=0,
            columns=(
                ("processado", "Processado", 145),
                ("cliente", "Cliente", 280),
                ("banco", "Banco", 160),
                ("valor", "Valor", 120),
                ("comprovante", "Comprovante", 180),
                ("verificacao", "Verificacao", 150),
                ("origem", "Origem", 150),
            ),
        )

    def _build_tree_area(
        self,
        master: tk.Misc,
        *,
        row: int,
        columns: tuple[tuple[str, str, int], ...],
    ) -> ttk.Treeview:
        frame = tk.Frame(master, bg=PALETTE["surface"], highlightbackground=PALETTE["border"], highlightthickness=1)
        frame.grid(row=row, column=0, sticky="nsew", padx=10, pady=10)
        frame.grid_rowconfigure(0, weight=1)
        frame.grid_columnconfigure(0, weight=1)

        tree = ttk.Treeview(frame, columns=[name for name, _title, _width in columns], show="headings")
        for name, title, width in columns:
            tree.heading(name, text=title)
            tree.column(name, width=width, anchor="w")

        y_scroll = ttk.Scrollbar(frame, orient="vertical", command=tree.yview)
        x_scroll = ttk.Scrollbar(frame, orient="horizontal", command=tree.xview)
        tree.configure(yscrollcommand=y_scroll.set, xscrollcommand=x_scroll.set)
        tree.grid(row=0, column=0, sticky="nsew")
        y_scroll.grid(row=0, column=1, sticky="ns")
        x_scroll.grid(row=1, column=0, sticky="ew")
        return tree

    def _build_footer(self) -> None:
        footer = tk.Frame(self, bg="#dfe8f2", padx=18, pady=12)
        footer.pack(fill="x", side="bottom")

        self.last_refresh_label = tk.Label(
            footer,
            text="Ultima atualizacao: -",
            bg="#dfe8f2",
            fg=PALETTE["text"],
            font=("Segoe UI", 9, "bold"),
        )
        self.last_refresh_label.pack(anchor="w")

        self.summary_label = tk.Label(
            footer,
            text="-",
            bg="#dfe8f2",
            fg=PALETTE["text"],
            justify="left",
            anchor="w",
            font=("Segoe UI", 9),
        )
        self.summary_label.pack(anchor="w", pady=(6, 0))

        self.error_label = tk.Label(
            footer,
            text="",
            bg="#dfe8f2",
            fg="#b42318",
            justify="left",
            anchor="w",
            font=("Segoe UI", 9, "bold"),
        )
        self.error_label.pack(anchor="w", pady=(6, 0))

        self.action_label = tk.Label(
            footer,
            text="",
            bg="#dfe8f2",
            fg="#065f46",
            justify="left",
            anchor="w",
            font=("Segoe UI", 9, "bold"),
        )
        self.action_label.pack(anchor="w", pady=(6, 0))

    def _render_ui_toggle_button(self, enabled: bool) -> None:
        self._ui_force_runtime_enabled = bool(enabled)
        if enabled:
            self.ui_toggle_button.configure(
                text="Auto clique: ON",
                bg="#0f766e",
                fg="#ffffff",
                activebackground="#0f766e",
                activeforeground="#ffffff",
            )
        else:
            self.ui_toggle_button.configure(
                text="Auto clique: OFF (manual)",
                bg="#b45309",
                fg="#ffffff",
                activebackground="#b45309",
                activeforeground="#ffffff",
            )

    def _render_filter_buttons(self) -> None:
        current = self.queue_filter_var.get()
        for key, button in self.filter_buttons.items():
            selected = key == current
            button.configure(
                bg="#dbeafe" if selected else "#e7eef6",
                fg=PALETTE["info"] if selected else PALETTE["muted"],
                activebackground="#dbeafe" if selected else "#e7eef6",
                activeforeground=PALETTE["info"] if selected else PALETTE["muted"],
            )

    def _on_queue_search_change(self, *_args: Any) -> None:
        self._apply_queue_filters()

    def _set_queue_filter(self, value: str) -> None:
        self.queue_filter_var.set(value)
        self._render_filter_buttons()
        self._apply_queue_filters()

    def toggle_ui_force_runtime(self) -> None:
        target = not self._ui_force_runtime_enabled
        ok, message = set_ui_force_runtime_enabled(self.base_dir, target)
        if ok:
            self.action_label.configure(text=message)
            self.error_label.configure(text="")
        else:
            self.error_label.configure(text=message)
        self.refresh_now()

    def clear_queue_now(self) -> None:
        confirm = messagebox.askyesno(
            "Ignorar fila antiga",
            "Ignorar tudo o que esta na fila agora?\nDepois disso, somente itens novos serao processados.",
            icon="warning",
        )
        if not confirm:
            return
        ok, message = clear_queue_backlog(self.base_dir)
        if ok:
            self.action_label.configure(text=message)
            self.error_label.configure(text="")
        else:
            self.error_label.configure(text=message)
        self.refresh_now()

    def stop_processing_now(self) -> None:
        confirm = messagebox.askyesno(
            "Parar processamento",
            "Parar o daemon agora?\n"
            "Ele deixara de processar ate voce iniciar novamente.",
            icon="warning",
        )
        if not confirm:
            return
        ok, message = stop_daemon_processing(self.base_dir)
        if ok:
            self.action_label.configure(text=message)
            self.error_label.configure(text="")
        else:
            self.error_label.configure(text=message)
        self.refresh_now()

    def restart_processing_now(self) -> None:
        confirm = messagebox.askyesno(
            "Reiniciar processamento",
            "Parar e iniciar o daemon novamente agora?",
            icon="warning",
        )
        if not confirm:
            return
        ok, message = restart_daemon_processing(self.base_dir)
        if ok:
            self.action_label.configure(text=message)
            self.error_label.configure(text="")
        else:
            self.error_label.configure(text=message)
        self.refresh_now()

    def _replace_tree_rows(
        self,
        tree: ttk.Treeview,
        rows: list[dict[str, Any]],
        visible_keys: list[str],
        path_map: Optional[dict[str, str]] = None,
    ) -> None:
        for item_id in tree.get_children():
            tree.delete(item_id)
        if path_map is not None:
            path_map.clear()
        for index, row in enumerate(rows):
            values = [row.get(key, "-") for key in visible_keys]
            item_id = tree.insert("", "end", iid=f"row-{index}", values=values)
            if path_map is not None:
                path_map[item_id] = str(row.get("open_path", "")).strip()

    def _render_log(self, lines: list[str]) -> None:
        self.log_text.configure(state="normal")
        self.log_text.delete("1.0", "end")
        self.log_text.insert("1.0", "\n".join(lines) if lines else "Sem linhas recentes de log.")
        self.log_text.configure(state="disabled")

    def _build_log_tab(self) -> None:
        tab = tk.Frame(self.notebook, bg=PALETTE["surface"])
        self.notebook.add(tab, text="Log recente")
        self.log_text = tk.Text(
            tab,
            wrap="none",
            bg="#0b1220",
            fg="#dbe4f0",
            insertbackground="#dbe4f0",
            relief="flat",
            font=("Consolas", 10),
        )
        self.log_text.pack(fill="both", expand=True)
        self.log_text.configure(state="disabled")

    def _open_path(self, path: Path | str) -> None:
        raw = str(path or "").strip()
        if not raw:
            return
        try:
            target = Path(raw)
        except Exception:
            return
        try:
            if target.exists():
                os.startfile(str(target if target.is_dir() else target.parent))
            else:
                os.startfile(str(target.parent))
        except Exception:
            pass

    def _apply_queue_filters(self, selected_file_id: Optional[str] = None) -> None:
        current_search = self.queue_search_var.get().strip().lower()
        current_filter = self.queue_filter_var.get()
        rows: list[dict[str, Any]] = []
        for row in self._queue_rows:
            bucket = str(row.get("queue_bucket") or "all")
            haystack = " ".join(
                [
                    str(row.get("grupo", "")),
                    str(row.get("arquivo", "")),
                    str(row.get("motivo", "")),
                    str(row.get("technical_code", "")),
                ]
            ).lower()
            if current_filter != "all" and bucket != current_filter:
                continue
            if current_search and current_search not in haystack:
                continue
            rows.append(row)

        for item_id in self.queue_tree.get_children():
            self.queue_tree.delete(item_id)
        self._queue_item_rows.clear()
        self._queue_item_paths.clear()
        for index, row in enumerate(rows):
            item_id = f"queue-{index}"
            values = [
                row.get("grupo", "-"),
                row.get("arquivo", "-"),
                row.get("arquivo_estado", "-"),
                row.get("mensagem_estado", "-"),
                row.get("motivo", "-"),
                row.get("tentativas", "-"),
                row.get("proximo", "-"),
            ]
            self.queue_tree.insert("", "end", iid=item_id, values=values, tags=(row.get("row_tag", "normal"),))
            self._queue_item_rows[item_id] = row
            self._queue_item_paths[item_id] = str(row.get("open_path", "")).strip()

        filter_label = dict(QUEUE_FILTERS).get(current_filter, "Todos")
        self.queue_count_label.configure(text=f"{len(rows)} item(ns) visiveis em '{filter_label}'")

        target_file_id = selected_file_id or ""
        if not target_file_id:
            current_row = self._selected_queue_row()
            target_file_id = str(current_row.get("file_id", "")) if current_row else ""
        if target_file_id:
            for item_id, row in self._queue_item_rows.items():
                if str(row.get("file_id", "")) == target_file_id:
                    self.queue_tree.selection_set(item_id)
                    self.queue_tree.focus(item_id)
                    self.queue_tree.see(item_id)
                    break
        self._on_queue_selection_change()

    def _selected_queue_row(self) -> Optional[dict[str, Any]]:
        selection = self.queue_tree.selection()
        if not selection:
            return None
        return self._queue_item_rows.get(selection[0])

    def _set_detail_row_values(self, row: Optional[dict[str, Any]]) -> None:
        if row is None:
            for var in self.detail_vars.values():
                var.set("-")
            self.detail_path_label.configure(text="-")
            self.detail_code_label.configure(text="-")
            self.queue_selection_label.configure(text="Nenhum item selecionado")
            self.open_selection_button.configure(state="disabled")
            self.show_selection_button.configure(state="disabled")
            self.ignore_selection_button.configure(state="disabled")
            return

        for key in self.detail_vars:
            self.detail_vars[key].set(str(row.get(key, "-") or "-"))
        self.detail_path_label.configure(text=str(row.get("full_path", "-") or "-"))
        self.detail_code_label.configure(text=str(row.get("technical_code", "-") or "-"))
        self.queue_selection_label.configure(text=f"Selecionado: {row.get('grupo', '-')} | {row.get('arquivo', '-')}")
        self.open_selection_button.configure(state="normal")
        self.show_selection_button.configure(state="normal")
        self.ignore_selection_button.configure(state="normal")

    def _on_queue_selection_change(self, _event: Any = None) -> None:
        self._set_detail_row_values(self._selected_queue_row())

    def _show_selected_details(self) -> None:
        row = self._selected_queue_row()
        if row is None:
            return
        top = tk.Toplevel(self)
        top.title("Detalhes do item")
        top.geometry("760x440")
        top.configure(bg=PALETTE["bg"])
        top.transient(self)

        frame = tk.Frame(top, bg=PALETTE["surface"], padx=14, pady=14, highlightbackground=PALETTE["border"], highlightthickness=1)
        frame.pack(fill="both", expand=True, padx=14, pady=14)
        tk.Label(frame, text="Detalhes completos", bg=PALETTE["surface"], fg=PALETTE["text"], font=("Segoe UI", 12, "bold")).pack(anchor="w")
        body = tk.Text(frame, wrap="word", relief="flat", bg=PALETTE["surface"], fg=PALETTE["text"], font=("Consolas", 10))
        body.pack(fill="both", expand=True, pady=(10, 0))
        body.insert(
            "1.0",
            "\n".join(
                [
                    f"Grupo: {row.get('grupo', '-')}",
                    f"Arquivo: {row.get('arquivo', '-')}",
                    f"Tipo: {row.get('tipo', '-')}",
                    f"Status do arquivo: {row.get('arquivo_estado', '-')}",
                    f"Status da mensagem: {row.get('mensagem_estado', '-')}",
                    f"Motivo atual: {row.get('motivo', '-')}",
                    f"Tentativas: {row.get('tentativas', '-')}",
                    f"Idade: {row.get('idade', '-')}",
                    f"Proxima tentativa: {row.get('proximo', '-')}",
                    f"Ultimo retorno UI: {row.get('last_ui_result', '-')}",
                    "",
                    f"Caminho completo: {row.get('full_path', '-')}",
                    f"Codigo tecnico: {row.get('technical_code', '-')}",
                    f"File ID: {row.get('file_id', '-')}",
                    f"MsgSvrID: {row.get('msg_svr_id', '-') or '-'}",
                ]
            ),
        )
        body.configure(state="disabled")

    def _open_selected_queue_item(self, _event: Any = None) -> None:
        row = self._selected_queue_row()
        if row is None:
            return
        self._open_path(row.get("open_path", ""))

    def _open_selected_message_item(self, _event: Any = None) -> None:
        selection = self.message_tree.selection()
        if not selection:
            return
        self._open_path(self._message_item_paths.get(selection[0], ""))

    def _open_queue_context_menu(self, event: Any) -> None:
        row_id = self.queue_tree.identify_row(event.y)
        if row_id:
            self.queue_tree.selection_set(row_id)
            self.queue_tree.focus(row_id)
            self._on_queue_selection_change()
        if self._selected_queue_row() is None:
            return
        try:
            self.queue_menu.tk_popup(event.x_root, event.y_root)
        finally:
            self.queue_menu.grab_release()

    def ignore_selected_queue_item_now(self) -> None:
        row = self._selected_queue_row()
        if row is None:
            return
        confirm = messagebox.askyesno(
            "Ignorar mensagem selecionada",
            "Ignorar somente esta mensagem da fila?\nOs itens ligados a ela tambem serao limpos para nao deixar a ordem travada.",
            icon="warning",
        )
        if not confirm:
            return
        ok, message = ignore_selected_queue_item(self.base_dir, str(row.get("file_id", "")))
        if ok:
            self.action_label.configure(text=message)
            self.error_label.configure(text="")
        else:
            self.error_label.configure(text=message)
        self.refresh_now()

    def refresh_now(self) -> None:
        previous_row = self._selected_queue_row()
        previous_file_id = str(previous_row.get("file_id", "")) if previous_row else ""
        snapshot = load_snapshot(self.base_dir)
        self._snapshot = snapshot
        self._queue_rows = snapshot.queue_rows

        self.status_label.configure(
            text=snapshot.daemon_status,
            bg=PALETTE["success"] if snapshot.daemon_running else "#9f1239",
        )
        self._render_ui_toggle_button(snapshot.ui_force_runtime_enabled)
        for title, _accent in METRIC_SPECS:
            self.metric_cards[title].set_value(snapshot.metrics.get(title, "-"))

        self._replace_tree_rows(
            self.message_tree,
            snapshot.message_rows,
            ["envio", "grupo", "estado", "ui_try", "ultimo_ui", "arquivo"],
            self._message_item_paths,
        )
        self._replace_tree_rows(
            self.receipt_tree,
            snapshot.receipt_rows,
            ["processado", "cliente", "banco", "valor", "comprovante", "verificacao", "origem"],
        )
        self._render_log(snapshot.log_lines)
        self._apply_queue_filters(selected_file_id=previous_file_id)

        self.last_refresh_label.configure(text=f"Ultima atualizacao: {fmt_dt(time.time())}")
        ui_mode = "ON" if snapshot.ui_force_runtime_enabled else "OFF (manual)"
        self.summary_label.configure(
            text=(
                f"Auto clique WeChat: {ui_mode} | "
                f"Ultimo UI: {short_text(snapshot.last_ui_result, 60)} | "
                f"Grupo UI: {short_text(snapshot.last_ui_talker, 36)} | "
                f"Ultima resolucao: {short_text(snapshot.last_resolution, 28)} | "
                f"Ultima verificacao: {short_text(snapshot.last_verification, 24)} | "
                f"Ultima excecao: {short_text(snapshot.last_exception, 40)}"
            )
        )
        self.error_label.configure(text=f"Erro de leitura: {snapshot.error}" if snapshot.error else "")

        if self._job is not None:
            self.after_cancel(self._job)
        self._job = self.after(REFRESH_SECONDS * 1000, self.refresh_now)


def main() -> int:
    base_dir = Path(__file__).resolve().parent
    app = DashboardApp(base_dir)
    app.mainloop()
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
