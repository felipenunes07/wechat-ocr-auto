from __future__ import annotations

import re
import time
import unicodedata
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, Optional

from pywinauto import Desktop
from pywinauto.application import Application
from pywinauto.keyboard import send_keys


MAIN_WINDOW_CLASS = "WeChatMainWndForPC"
MAIN_WINDOW_RETRY_ATTEMPTS = 3
MAIN_WINDOW_RETRY_DELAY_SECONDS = 1.5
CLASSIC_VIEWER_CLASSES = {"WeChatAppEx", "Chrome_WidgetWin_0"}
HOVER_WINDOW_CLASSES = {"HttpImgHoverWnd", "Search2Wnd"}
CONVERSATION_PARENT_MARKERS = {"会话", "conversation"}
WEB_SEARCH_MARKERS = {
    "@str:ids_fav_search_result:3780",
    "search result",
    "research",
}
PHOTO_MARKERS = {"[photo]", "photo"}
MENU_OPEN_MARKERS = {"open", "preview", "save", "打开", "查看", "另存", "保存"}
TEMP_EXTENSIONS = {".jpg", ".jpeg", ".png", ".bmp", ".webp"}
IMAGE_EXTENSIONS = TEMP_EXTENSIONS | {".dat"}


@dataclass
class UIMessageCandidate:
    msg_svr_id: str
    talker: str
    talker_display: str
    expected_image_path: Path
    thumb_path: Path
    create_time: float


@dataclass
class UIForceBatchResult:
    ok: bool
    opened_chat: bool
    opened_viewer: bool
    used_direction: Optional[str]
    resolved_msg_ids: list[str] = field(default_factory=list)
    resolved_media_paths: dict[str, str] = field(default_factory=dict)
    resolved_sources: dict[str, str] = field(default_factory=dict)
    note: str = ""


@dataclass
class MainWindowProbeResult:
    window: Optional[Any]
    ready: bool
    note: str


def normalize_ui_text(value: Any) -> str:
    raw = str(value or "")
    raw = raw.replace("\ufffd", " ").replace("\u25a1", " ")
    raw = unicodedata.normalize("NFKD", raw)
    raw = "".join(ch for ch in raw if not unicodedata.combining(ch))
    raw = re.sub(r"\s+", " ", raw).strip().lower()
    return raw


def build_search_candidates(display_name: str) -> list[str]:
    raw = str(display_name or "").strip()
    variants: list[str] = []
    if raw:
        variants.append(raw)

    ascii_tokens = re.findall(r"[A-Za-z0-9]{3,}", raw)
    if ascii_tokens:
        longest = max(ascii_tokens, key=len)
        variants.insert(0, longest)
        compact = " ".join(ascii_tokens)
        if compact not in variants:
            variants.append(compact)

    normalized = normalize_ui_text(raw)
    normalized_tokens = re.findall(r"[a-z0-9]{3,}", normalized)
    if normalized_tokens:
        longest_norm = max(normalized_tokens, key=len)
        if longest_norm not in variants:
            variants.insert(0, longest_norm)

    out: list[str] = []
    seen: set[str] = set()
    for item in variants:
        item = str(item or "").strip()
        if not item:
            continue
        key = normalize_ui_text(item)
        if key and key not in seen:
            out.append(item)
            seen.add(key)
    return out or [raw]


class WeChatUIForceDownloader:
    def __init__(self, focus_policy: str = "immediate", item_timeout_seconds: int = 5) -> None:
        self.focus_policy = str(focus_policy or "immediate").strip().lower() or "immediate"
        self.item_timeout_seconds = max(1, int(item_timeout_seconds))
        self.desktop = Desktop(backend="uia")

    def _safe_window_text(self, wrapper: Any) -> str:
        try:
            return str(wrapper.window_text() or "")
        except Exception:
            try:
                return str(getattr(wrapper.element_info, "name", "") or "")
            except Exception:
                return ""

    def _safe_class_name(self, wrapper: Any) -> str:
        try:
            return str(wrapper.class_name() or "")
        except Exception:
            try:
                return str(getattr(wrapper.element_info, "class_name", "") or "")
            except Exception:
                return ""

    def _safe_rectangle(self, wrapper: Any) -> Optional[Any]:
        try:
            rect = wrapper.rectangle()
        except Exception:
            return None
        try:
            if rect.width() <= 0 or rect.height() <= 0:
                return None
        except Exception:
            return None
        return rect

    def _is_visible(self, wrapper: Any) -> bool:
        try:
            return bool(wrapper.is_visible())
        except Exception:
            return False

    def _window_area(self, wrapper: Any) -> int:
        rect = self._safe_rectangle(wrapper)
        if rect is None:
            return 0
        try:
            return max(0, int(rect.width())) * max(0, int(rect.height()))
        except Exception:
            return 0

    def _compact_text(self, value: Any, limit: int = 48) -> str:
        text = re.sub(r"\s+", " ", str(value or "")).strip().replace("|", "/")
        if not text:
            return "?"
        return text[:limit]

    def _describe_wrapper(self, wrapper: Any) -> str:
        cls_name = self._compact_text(self._safe_class_name(wrapper), limit=32)
        title = self._compact_text(self._safe_window_text(wrapper), limit=48)
        visible = "1" if self._is_visible(wrapper) else "0"
        return f"{cls_name}:{title}:vis={visible}"

    def _summarize_candidates(self, windows: list[Any], limit: int = 2) -> str:
        if not windows:
            return "none"
        ordered = sorted(windows, key=self._window_area, reverse=True)
        return ",".join(self._describe_wrapper(win) for win in ordered[: max(1, limit)])

    def _restore_window(self, wrapper: Any) -> bool:
        try:
            wrapper.restore()
            time.sleep(0.25)
        except Exception:
            return self._is_visible(wrapper)
        return self._is_visible(wrapper)

    def _focus_wrapper(self, wrapper: Any) -> None:
        try:
            wrapper.set_focus()
            return
        except Exception:
            pass
        try:
            wrapper.click_input()
        except Exception:
            pass

    def _probe_main_window_once(self) -> MainWindowProbeResult:
        windows = []
        lookup_note: Optional[str] = None
        try:
            windows = self.desktop.windows(class_name=MAIN_WINDOW_CLASS, visible_only=False)
        except Exception as exc:
            windows = []
            lookup_note = f"class_lookup_err={type(exc).__name__}:{self._compact_text(exc, limit=72)}"
        ordered = sorted(windows, key=self._window_area, reverse=True)
        visible = [win for win in ordered if self._is_visible(win)]
        sample = self._summarize_candidates(ordered)
        if visible:
            return MainWindowProbeResult(
                window=visible[0],
                ready=True,
                note=f"source=class_visible|class_count={len(windows)}|sample={sample}",
            )
        if ordered:
            restored = self._restore_window(ordered[0])
            return MainWindowProbeResult(
                window=ordered[0],
                ready=restored or self._is_visible(ordered[0]),
                note=(
                    f"source=class_hidden|class_count={len(windows)}|restore={'ok' if restored else 'no'}"
                    f"|sample={sample}"
                ),
            )
        try:
            app = Application(backend="uia").connect(path="WeChat.exe")
            win = app.top_window()
            if win is None:
                note_parts = [f"class_count={len(windows)}", f"sample={sample}", "fallback=top_window_none"]
                if lookup_note:
                    note_parts.insert(0, lookup_note)
                return MainWindowProbeResult(window=None, ready=False, note="|".join(note_parts))
            restored = self._restore_window(win)
            return MainWindowProbeResult(
                window=win,
                ready=restored or self._is_visible(win),
                note=(
                    f"source=process_connect|class_count={len(windows)}|restore={'ok' if restored else 'no'}"
                    f"|fallback={self._describe_wrapper(win)}"
                ),
            )
        except Exception as exc:
            note_parts = [f"class_count={len(windows)}", f"sample={sample}"]
            if lookup_note:
                note_parts.insert(0, lookup_note)
            note_parts.append(f"fallback_err={type(exc).__name__}:{self._compact_text(exc, limit=72)}")
            return MainWindowProbeResult(window=None, ready=False, note="|".join(note_parts))

    def _probe_main_window(
        self,
        retries: int = MAIN_WINDOW_RETRY_ATTEMPTS,
        retry_delay: float = MAIN_WINDOW_RETRY_DELAY_SECONDS,
    ) -> MainWindowProbeResult:
        attempts = max(1, int(retries))
        delay_seconds = max(0.1, float(retry_delay))
        last_result = MainWindowProbeResult(window=None, ready=False, note="main_window_probe_not_started")
        for attempt in range(1, attempts + 1):
            result = self._probe_main_window_once()
            result.note = f"attempt={attempt}/{attempts}|{result.note}"
            if result.window is not None:
                return result
            last_result = result
            if attempt < attempts:
                time.sleep(delay_seconds)
        return last_result

    def probe_main_window(
        self,
        retries: int = MAIN_WINDOW_RETRY_ATTEMPTS,
        retry_delay: float = MAIN_WINDOW_RETRY_DELAY_SECONDS,
    ) -> tuple[bool, str]:
        result = self._probe_main_window(retries=retries, retry_delay=retry_delay)
        return bool(result.ready), result.note

    def _main_window(self) -> Any:
        result = self._probe_main_window()
        if result.window is not None:
            return result.window
        raise RuntimeError(f"wechat_main_window_not_found|{result.note}")

    def _focus_main_window(self, win: Any) -> None:
        try:
            if self.focus_policy == "immediate":
                win.restore()
        except Exception:
            pass
        self._focus_wrapper(win)
        time.sleep(0.25)

    def _all_parent_texts(self, wrapper: Any) -> list[str]:
        texts: list[str] = []
        current = wrapper
        for _ in range(8):
            if current is None:
                break
            texts.append(self._safe_window_text(current))
            try:
                current = current.parent()
            except Exception:
                break
        return texts

    def _search_edit(self, win: Any) -> Any:
        edits: list[Any] = []
        try:
            edits = win.descendants(control_type="Edit")
        except Exception:
            edits = []
        visible = [edit for edit in edits if self._is_visible(edit)]
        if not visible:
            raise RuntimeError("wechat_search_edit_not_found")
        visible.sort(key=lambda edit: (self._safe_rectangle(edit).top, self._safe_rectangle(edit).left))
        return visible[0]

    def _clear_and_type(self, edit: Any, term: str) -> None:
        self._focus_wrapper(edit)
        time.sleep(0.1)
        try:
            edit.click_input()
        except Exception:
            pass
        time.sleep(0.05)
        send_keys("^a{BACKSPACE}", pause=0.02)
        time.sleep(0.08)
        try:
            edit.type_keys(term, with_spaces=True, set_foreground=False, pause=0.02)
        except Exception:
            send_keys(term, with_spaces=True, pause=0.02)
        time.sleep(0.45)

    def _search_overlay_windows(self) -> list[Any]:
        out: list[Any] = []
        for class_name in HOVER_WINDOW_CLASSES:
            try:
                windows = self.desktop.windows(class_name=class_name)
            except Exception:
                windows = []
            for win in windows:
                if self._is_visible(win):
                    out.append(win)
        return out

    def _close_search_overlay(self) -> None:
        for _ in range(2):
            send_keys("{ESC}", pause=0.02)
            time.sleep(0.15)

    def _conversation_search_results(self, win: Any, candidate_key: str) -> list[Any]:
        items: list[Any] = []
        try:
            items = win.descendants(control_type="ListItem")
        except Exception:
            items = []

        scored: list[tuple[int, float, Any]] = []
        for item in items:
            if not self._is_visible(item):
                continue
            rect = self._safe_rectangle(item)
            if rect is None or rect.height() < 18:
                continue

            own_text = normalize_ui_text(self._safe_window_text(item))
            parent_texts = [normalize_ui_text(text) for text in self._all_parent_texts(item)]
            parent_joined = " | ".join(parent_texts)
            if any(marker in parent_joined for marker in WEB_SEARCH_MARKERS):
                continue
            if not any(marker in parent_joined for marker in CONVERSATION_PARENT_MARKERS):
                continue

            score = 0
            if candidate_key and candidate_key in own_text:
                score += 6
            if own_text and own_text in candidate_key:
                score += 5
            if self._safe_class_name(item) == "ListItem":
                score += 1
            center_bias = float(rect.bottom)
            scored.append((score, center_bias, item))

        scored.sort(key=lambda item: (item[0], item[1]), reverse=True)
        return [item for _score, _center, item in scored]

    def _header_matches(self, win: Any, display_name: str) -> bool:
        keys = [normalize_ui_text(term) for term in build_search_candidates(display_name)]
        keys = [key for key in keys if key]
        if not keys:
            return False

        texts: list[str] = []
        try:
            for child in win.descendants(control_type="Text"):
                if not self._is_visible(child):
                    continue
                rect = self._safe_rectangle(child)
                if rect is None or rect.top > 220:
                    continue
                texts.append(normalize_ui_text(self._safe_window_text(child)))
        except Exception:
            texts = []
        blob = " ".join(texts)
        return any(key in blob for key in keys)

    def open_chat(self, display_name: str) -> tuple[bool, str]:
        win = self._main_window()
        self._focus_main_window(win)
        edit = self._search_edit(win)
        last_term = ""
        for term in build_search_candidates(display_name):
            last_term = term
            self._clear_and_type(edit, term)
            results = self._conversation_search_results(win, normalize_ui_text(term))
            if not results:
                continue
            opened = False
            try:
                results[0].click_input()
                opened = True
            except Exception:
                try:
                    results[0].invoke()
                    opened = True
                except Exception:
                    try:
                        results[0].double_click_input()
                        opened = True
                    except Exception:
                        opened = False
            if not opened:
                continue
            time.sleep(0.35)
            self._close_search_overlay()
            time.sleep(0.2)
            if self._header_matches(win, display_name) or self._header_matches(win, term):
                return True, f"chat_opened:{term}"
            return True, f"chat_opened_unverified:{term}"
        return False, f"chat_not_found:{last_term or display_name}"

    def _visible_photo_items(self, win: Any) -> list[Any]:
        items: list[Any] = []
        try:
            items = win.descendants(control_type="ListItem")
        except Exception:
            items = []
        photos: list[Any] = []
        for item in items:
            if not self._is_visible(item):
                continue
            rect = self._safe_rectangle(item)
            if rect is None or rect.height() < 40:
                continue
            text = normalize_ui_text(self._safe_window_text(item))
            if not any(marker in text for marker in PHOTO_MARKERS):
                continue
            photos.append(item)
        photos.sort(key=lambda item: self._safe_rectangle(item).bottom, reverse=True)
        return photos

    def _largest_inner_button(self, photo_item: Any) -> Any:
        best = photo_item
        best_area = 0
        try:
            buttons = photo_item.descendants(control_type="Button")
        except Exception:
            buttons = []
        for button in buttons:
            if not self._is_visible(button):
                continue
            rect = self._safe_rectangle(button)
            if rect is None:
                continue
            area = rect.width() * rect.height()
            if area > best_area:
                best = button
                best_area = area
        return best

    def _classic_viewer_window(self, main_handle: int) -> Optional[Any]:
        try:
            windows = self.desktop.windows()
        except Exception:
            windows = []
        for win in windows:
            if not self._is_visible(win):
                continue
            try:
                if int(win.handle) == int(main_handle):
                    continue
            except Exception:
                pass
            if self._safe_class_name(win) in CLASSIC_VIEWER_CLASSES:
                return win
        return None

    def _hover_windows(self) -> list[Any]:
        out: list[Any] = []
        for win in self._search_overlay_windows():
            if self._safe_class_name(win) in HOVER_WINDOW_CLASSES:
                out.append(win)
        return out

    def _click_center(self, wrapper: Any, double: bool = False, right: bool = False) -> None:
        rect = self._safe_rectangle(wrapper)
        if rect is None:
            raise RuntimeError("wrapper_without_rectangle")
        if right:
            wrapper.right_click_input(coords=(max(2, rect.width() // 2), max(2, rect.height() // 2)))
            return
        if double:
            wrapper.double_click_input(coords=(max(2, rect.width() // 2), max(2, rect.height() // 2)))
            return
        wrapper.click_input(coords=(max(2, rect.width() // 2), max(2, rect.height() // 2)))

    def open_photo_by_inner_button(self, photo_item: Any, main_handle: int) -> tuple[bool, bool]:
        target = self._largest_inner_button(photo_item)
        hover_seen = False
        self._focus_wrapper(photo_item)
        time.sleep(0.08)

        try:
            self._click_center(target, double=False, right=False)
        except Exception:
            try:
                target.click_input()
            except Exception:
                pass
        time.sleep(0.35)
        if self._classic_viewer_window(main_handle) is not None:
            return True, hover_seen
        hover_seen = hover_seen or bool(self._hover_windows())

        try:
            self._click_center(target, double=True, right=False)
        except Exception:
            try:
                target.double_click_input()
            except Exception:
                pass
        time.sleep(0.45)
        if self._classic_viewer_window(main_handle) is not None:
            return True, hover_seen
        hover_seen = hover_seen or bool(self._hover_windows())
        return False, hover_seen

    def open_photo_by_hover_window(self) -> bool:
        attempted = False
        for hover in self._hover_windows():
            attempted = True
            try:
                self._click_center(hover)
            except Exception:
                pass
            time.sleep(0.18)
            send_keys("{ENTER}", pause=0.02)
            time.sleep(0.18)
        if self._hover_windows():
            send_keys(" ", pause=0.02)
            time.sleep(0.12)
            send_keys("{ENTER}", pause=0.02)
            time.sleep(0.2)
        return attempted

    def _context_menu_action(self, target: Any) -> str:
        try:
            self._click_center(target, right=True)
        except Exception:
            try:
                target.right_click_input()
            except Exception:
                return "photo_open_action_failed"
        time.sleep(0.35)

        menu_windows: list[Any] = []
        try:
            menu_windows = self.desktop.windows(control_type="Menu")
        except Exception:
            menu_windows = []
        try:
            menu_windows.extend([win for win in self.desktop.windows(class_name="#32768") if self._is_visible(win)])
        except Exception:
            pass

        for menu in menu_windows:
            if not self._is_visible(menu):
                continue
            try:
                items = menu.descendants(control_type="MenuItem")
            except Exception:
                items = []
            for item in items:
                text = normalize_ui_text(self._safe_window_text(item))
                if any(marker in text for marker in MENU_OPEN_MARKERS):
                    try:
                        item.click_input()
                    except Exception:
                        self._focus_wrapper(item)
                        send_keys("{ENTER}", pause=0.02)
                    time.sleep(0.3)
                    return "context_menu_action_invoked"

        send_keys("{ESC}", pause=0.02)
        return "context_menu_no_open_action"

    def _file_storage_root(self, path: Path) -> Optional[Path]:
        parts = list(path.parts)
        lowered = [part.lower() for part in parts]
        if "filestorage" not in lowered:
            return None
        idx = lowered.index("filestorage")
        return Path(*parts[: idx + 1])

    def _temp_dir_for_job(self, job: UIMessageCandidate) -> Optional[Path]:
        for base_path in (job.expected_image_path, job.thumb_path):
            root = self._file_storage_root(base_path)
            if root is not None:
                temp_dir = root / "Temp"
                if temp_dir.exists():
                    return temp_dir
        return None

    def _msgattach_image_dir_for_job(self, job: UIMessageCandidate) -> Optional[Path]:
        expected_parent = job.expected_image_path.parent
        if expected_parent.exists():
            return expected_parent
        thumb_parts = list(job.thumb_path.parts)
        lowered = [part.lower() for part in thumb_parts]
        if "thumb" in lowered:
            idx = lowered.index("thumb")
            thumb_parts[idx] = "Image"
            candidate = Path(*thumb_parts[:-1])
            if candidate.exists():
                return candidate
        return expected_parent if expected_parent else None

    def _job_tokens(self, job: UIMessageCandidate) -> set[str]:
        tokens: set[str] = set()
        for path in (job.thumb_path, job.expected_image_path):
            for value in (path.name, path.stem):
                norm = normalize_ui_text(value).replace(" ", "")
                if not norm:
                    continue
                tokens.add(norm)
                if norm.endswith("_t"):
                    tokens.add(norm[:-2])
                if norm.endswith("_"):
                    tokens.add(norm[:-1])
        return {token for token in tokens if len(token) >= 6}

    def _path_matches_tokens(self, path: Path, tokens: set[str]) -> bool:
        path_key = normalize_ui_text(path.name).replace(" ", "")
        if not path_key:
            return False
        return any(token and token in path_key for token in tokens)

    def collect_new_media_candidates(self, job: UIMessageCandidate, since_ts: float) -> list[tuple[Path, str]]:
        candidates: dict[str, tuple[Path, str]] = {}

        expected = job.expected_image_path
        if expected.exists():
            candidates[str(expected)] = (expected, "ui_expected_image")

        tokens = self._job_tokens(job)

        image_dir = self._msgattach_image_dir_for_job(job)
        if image_dir is not None and image_dir.exists():
            try:
                for child in image_dir.iterdir():
                    if not child.is_file() or child.suffix.lower() not in IMAGE_EXTENSIONS:
                        continue
                    try:
                        mtime = child.stat().st_mtime
                    except Exception:
                        continue
                    if mtime < (since_ts - 1.0):
                        continue
                    if self._path_matches_tokens(child, tokens):
                        candidates.setdefault(str(child), (child, "ui_msgattach_image_correlated"))
            except Exception:
                pass

        temp_dir = self._temp_dir_for_job(job)
        if temp_dir is not None and temp_dir.exists():
            try:
                for child in temp_dir.iterdir():
                    if not child.is_file() or child.suffix.lower() not in TEMP_EXTENSIONS:
                        continue
                    try:
                        mtime = child.stat().st_mtime
                    except Exception:
                        continue
                    if mtime < (since_ts - 1.0):
                        continue
                    if self._path_matches_tokens(child, tokens):
                        candidates.setdefault(str(child), (child, "ui_temp_correlated"))
            except Exception:
                pass

        exact = [item for item in candidates.values() if item[1] == "ui_expected_image"]
        if exact:
            return exact[:1]

        only_msgattach = [item for item in candidates.values() if item[1] == "ui_msgattach_image_correlated"]
        if len(only_msgattach) == 1:
            return only_msgattach

        only_temp = [item for item in candidates.values() if item[1] == "ui_temp_correlated"]
        if len(only_temp) == 1 and not only_msgattach:
            return only_temp

        if len(candidates) == 1:
            return list(candidates.values())
        return []

    def resolve_media_from_ui_effect(self, job: UIMessageCandidate, started_at: float, timeout_seconds: int = 6) -> tuple[Optional[Path], Optional[str]]:
        deadline = time.time() + max(1, int(timeout_seconds))
        while time.time() <= deadline:
            candidates = self.collect_new_media_candidates(job, started_at)
            if len(candidates) == 1:
                resolved_path, source = candidates[0]
                if resolved_path.exists():
                    return resolved_path, source
            time.sleep(0.25)
        return None, None

    def _close_viewer(self) -> None:
        send_keys("{ESC}", pause=0.02)
        time.sleep(0.2)

    def _process_job_from_chat(self, job: UIMessageCandidate, photo_item: Any, main_handle: int) -> tuple[Optional[Path], Optional[str], str, bool]:
        started_at = time.time()
        viewer_opened, hover_seen = self.open_photo_by_inner_button(photo_item, main_handle)
        resolved_path, resolved_source = self.resolve_media_from_ui_effect(job, started_at, timeout_seconds=self.item_timeout_seconds + 1)
        if resolved_path is not None and resolved_source is not None:
            return resolved_path, resolved_source, resolved_source, viewer_opened

        hover_attempted = False
        if not viewer_opened:
            hover_attempted = self.open_photo_by_hover_window()
            resolved_path, resolved_source = self.resolve_media_from_ui_effect(job, started_at, timeout_seconds=self.item_timeout_seconds + 1)
            if resolved_path is not None and resolved_source is not None:
                return resolved_path, resolved_source, resolved_source, viewer_opened

        target = self._largest_inner_button(photo_item)
        context_note = self._context_menu_action(target)
        resolved_path, resolved_source = self.resolve_media_from_ui_effect(job, started_at, timeout_seconds=self.item_timeout_seconds + 1)
        if resolved_path is not None and resolved_source is not None:
            return resolved_path, resolved_source, resolved_source, viewer_opened

        if context_note == "context_menu_no_open_action":
            return None, None, "context_menu_no_open_action", viewer_opened
        if hover_seen or hover_attempted or self._hover_windows():
            return None, None, "hover_without_materialized_media", viewer_opened
        return None, None, "photo_open_action_failed", viewer_opened

    def force_download_batch(self, jobs: list[UIMessageCandidate]) -> UIForceBatchResult:
        if not jobs:
            return UIForceBatchResult(
                ok=False,
                opened_chat=False,
                opened_viewer=False,
                used_direction=None,
                note="empty_ui_batch",
            )

        win = self._main_window()
        self._focus_main_window(win)
        opened_chat, open_note = self.open_chat(jobs[0].talker_display)
        if not opened_chat:
            return UIForceBatchResult(
                ok=False,
                opened_chat=False,
                opened_viewer=False,
                used_direction=None,
                note=open_note,
            )

        resolved_ids: list[str] = []
        resolved_paths: dict[str, str] = {}
        resolved_sources: dict[str, str] = {}
        opened_viewer = False
        last_failure_note = "photo_items_not_found"

        for index, job in enumerate(jobs):
            current_win = self._main_window()
            photo_items = self._visible_photo_items(current_win)
            if not photo_items:
                last_failure_note = "photo_items_not_found"
                break
            photo_item = photo_items[min(index, len(photo_items) - 1)]
            resolved_path, resolved_source, note, viewer_seen = self._process_job_from_chat(job, photo_item, int(current_win.handle))
            opened_viewer = opened_viewer or viewer_seen
            if viewer_seen:
                self._close_viewer()
            if resolved_path is not None and resolved_source is not None:
                resolved_ids.append(job.msg_svr_id)
                resolved_paths[job.msg_svr_id] = str(resolved_path)
                resolved_sources[job.msg_svr_id] = resolved_source
                last_failure_note = resolved_source
                continue
            last_failure_note = note

        if resolved_ids:
            unique_sources = sorted({resolved_sources[msg_id] for msg_id in resolved_ids if msg_id in resolved_sources})
            note = unique_sources[0] if len(unique_sources) == 1 else "ui_force_batch_partial"
            return UIForceBatchResult(
                ok=True,
                opened_chat=True,
                opened_viewer=opened_viewer,
                used_direction=None,
                resolved_msg_ids=resolved_ids,
                resolved_media_paths=resolved_paths,
                resolved_sources=resolved_sources,
                note=note,
            )

        return UIForceBatchResult(
            ok=False,
            opened_chat=True,
            opened_viewer=opened_viewer,
            used_direction=None,
            note=last_failure_note,
        )
