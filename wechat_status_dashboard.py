from __future__ import annotations

import os
import sqlite3
import subprocess
import time
from collections import deque
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path
from typing import Any, Optional

import tkinter as tk
from tkinter import ttk


APP_TITLE = "Painel WeChat OCR"
REFRESH_SECONDS = 5
MAX_QUEUE_ROWS = 80
MAX_MESSAGE_ROWS = 120
MAX_RECEIPT_ROWS = 40
MAX_LOG_LINES = 24

WAITING_LABELS = {
    "WAITING_ORIGINAL_MEDIA": "Aguardando original",
    "WAITING_UI_FORCE_DOWNLOAD": "Baixando pelo WeChat",
    "WAITING_TEMP_CONTEXT": "Aguardando contexto temp",
    "WAITING_TEMP_DB_MATCH": "Aguardando match temp",
}

MESSAGE_STATE_LABELS = {
    "NEW": "Nova",
    "WAITING_ORIGINAL": "Aguardando original",
    "UI_FORCE_PENDING": "UI pendente",
    "UI_FORCE_RUNNING": "UI rodando",
    "RESOLVED": "Resolvida",
    "THUMB_FALLBACK": "Thumb fallback",
    "EXCEPTION": "Excecao",
}


@dataclass
class DashboardSnapshot:
    daemon_status: str
    daemon_running: bool
    metrics: dict[str, str]
    queue_rows: list[dict[str, str]]
    message_rows: list[dict[str, str]]
    receipt_rows: list[dict[str, str]]
    log_lines: list[str]
    last_ui_result: str
    last_ui_talker: str
    last_exception: str
    last_resolution: str
    last_verification: str
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


def compact_path(value: Any, limit: int = 68) -> str:
    raw = str(value or "").strip()
    if not raw:
        return "-"
    path = Path(raw)
    suffix = raw[-limit:] if len(raw) > limit else raw
    return path.name if len(path.name) <= limit else suffix


def wait_reason_label(last_error: Any) -> str:
    err = str(last_error or "").strip()
    if not err:
        return "-"
    if err.startswith("WAITING_PRIOR_MESSAGE_ORDER:"):
        return "Aguardando mensagem anterior"
    return WAITING_LABELS.get(err, short_text(err, limit=54))


def message_state_label(state: Any) -> str:
    return MESSAGE_STATE_LABELS.get(str(state or "").strip(), str(state or "-").strip() or "-")


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


def load_snapshot(base_dir: Path) -> DashboardSnapshot:
    db_path = base_dir / "wechat_receipt_state.db"
    log_path = base_dir / "wechat_receipt.out.log"
    pid_path = base_dir / "wechat_receipt.pid"

    daemon_status, daemon_running = process_status(pid_path)
    snapshot = DashboardSnapshot(
        daemon_status=daemon_status,
        daemon_running=daemon_running,
        metrics={},
        queue_rows=[],
        message_rows=[],
        receipt_rows=[],
        log_lines=read_tail_lines(log_path),
        last_ui_result="-",
        last_ui_talker="-",
        last_exception="-",
        last_resolution="-",
        last_verification="-",
    )

    if not db_path.exists():
        snapshot.error = f"Banco nao encontrado: {db_path}"
        return snapshot

    now = time.time()
    recent_floor = now - 24 * 3600
    try:
        with sqlite_connect_ro(db_path) as conn:
            cur = conn.cursor()
            message_jobs_exists = scalar(
                cur,
                "SELECT COUNT(*) FROM sqlite_master WHERE type='table' AND name='message_jobs'",
            ) > 0

            snapshot.metrics = {
                "Fila total": str(scalar(cur, "SELECT COUNT(*) FROM files WHERE status IN ('pending','retry','processing')")),
                "Aguard. original": str(
                    scalar(
                        cur,
                        "SELECT COUNT(*) FROM files WHERE status IN ('pending','retry','processing') AND last_error='WAITING_ORIGINAL_MEDIA'",
                    )
                ),
                "Aguard. ordem": str(
                    scalar(
                        cur,
                        "SELECT COUNT(*) FROM files WHERE status IN ('pending','retry','processing') AND last_error LIKE 'WAITING_PRIOR_MESSAGE_ORDER:%'",
                    )
                ),
                "UI pendente": str(
                    scalar(cur, "SELECT COUNT(*) FROM message_jobs WHERE state='UI_FORCE_PENDING'") if message_jobs_exists else 0
                ),
                "UI rodando": str(
                    scalar(cur, "SELECT COUNT(*) FROM message_jobs WHERE state='UI_FORCE_RUNNING'") if message_jobs_exists else 0
                ),
                "Processando": str(scalar(cur, "SELECT COUNT(*) FROM files WHERE status='processing'")),
                "Excecoes": str(scalar(cur, "SELECT COUNT(*) FROM files WHERE status='exception'")),
                "Receipts 24h": str(scalar(cur, "SELECT COUNT(*) FROM receipts WHERE ingested_at >= ?", (recent_floor,))),
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
                        (
                            SELECT COALESCE(NULLIF(mj.talker_display, ''), mj.talker)
                            FROM message_jobs mj
                            WHERE mj.thumb_path = f.path OR mj.expected_image_path = f.path
                            ORDER BY mj.last_seen_at DESC
                            LIMIT 1
                        ) AS talker,
                        (
                            SELECT mj.state
                            FROM message_jobs mj
                            WHERE mj.thumb_path = f.path OR mj.expected_image_path = f.path
                            ORDER BY mj.last_seen_at DESC
                            LIMIT 1
                        ) AS message_state
                    FROM files f
                    WHERE f.status IN ('pending', 'retry', 'processing')
                    ORDER BY f.first_seen ASC, f.mtime ASC
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
                        '' AS talker,
                        '' AS message_state
                    FROM files f
                    WHERE f.status IN ('pending', 'retry', 'processing')
                    ORDER BY f.first_seen ASC, f.mtime ASC
                    LIMIT ?
                """
            for row in cur.execute(queue_sql, (MAX_QUEUE_ROWS,)).fetchall():
                snapshot.queue_rows.append(
                    {
                        "grupo": short_text(row["talker"], limit=26),
                        "arquivo": compact_path(row["path"]),
                        "tipo": short_text(row["source_kind"], limit=18),
                        "fila": wait_reason_label(row["last_error"]),
                        "msg_estado": message_state_label(row["message_state"]),
                        "tentativas": str(int(row["attempts"] or 0)),
                        "idade": fmt_age(row["first_seen"]),
                        "proximo": fmt_dt(row["next_attempt"]),
                        "open_path": str(row["path"] or ""),
                    }
                )

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
                    WHERE state NOT IN ('RESOLVED', 'THUMB_FALLBACK', 'EXCEPTION')
                    ORDER BY create_time ASC, msg_svr_id ASC
                    LIMIT ?
                """
                for row in cur.execute(message_sql, (MAX_MESSAGE_ROWS,)).fetchall():
                    open_path = str(row["expected_image_path"] or row["thumb_path"] or "")
                    snapshot.message_rows.append(
                        {
                            "envio": fmt_dt(row["create_time"]),
                            "grupo": short_text(row["talker_name"], limit=32),
                            "estado": message_state_label(row["state"]),
                            "ui_try": str(int(row["ui_force_attempts"] or 0)),
                            "ultimo_ui": short_text(row["last_ui_result"], limit=34),
                            "arquivo": compact_path(open_path),
                            "open_path": open_path,
                        }
                    )

                snapshot.last_ui_result = scalar_text(cur, "SELECT value FROM meta WHERE key='last_ui_result' LIMIT 1") or "-"
                snapshot.last_ui_talker = scalar_text(cur, "SELECT value FROM meta WHERE key='last_ui_talker' LIMIT 1") or "-"
                snapshot.last_exception = scalar_text(cur, "SELECT value FROM meta WHERE key='last_exception_reason' LIMIT 1") or "-"
                snapshot.last_resolution = scalar_text(cur, "SELECT value FROM meta WHERE key='last_resolution_source' LIMIT 1") or "-"
                snapshot.last_verification = scalar_text(cur, "SELECT value FROM meta WHERE key='last_verification_status' LIMIT 1") or "-"

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
                amount_text = "-"
                if row["amount"] not in (None, ""):
                    amount_text = str(row["amount"])
                snapshot.receipt_rows.append(
                    {
                        "processado": fmt_dt(row["ingested_at"]),
                        "cliente": short_text(row["owner_name"], limit=30),
                        "banco": short_text(row["bank"], limit=16),
                        "valor": amount_text,
                        "comprovante": f"{row['txn_date']} {row['txn_time']}".strip(),
                        "verificacao": short_text(row["verification_status"], limit=18),
                        "origem": short_text(row["resolution_source"], limit=18),
                    }
                )
    except Exception as exc:
        snapshot.error = f"{type(exc).__name__}: {exc}"

    return snapshot


class MetricCard(tk.Frame):
    def __init__(self, master: tk.Misc, title: str) -> None:
        super().__init__(master, bg="#ffffff", bd=1, relief="solid", padx=10, pady=8)
        self.title_label = tk.Label(
            self,
            text=title,
            bg="#ffffff",
            fg="#455065",
            font=("Segoe UI", 9, "bold"),
            anchor="w",
        )
        self.title_label.pack(fill="x")
        self.value_label = tk.Label(
            self,
            text="-",
            bg="#ffffff",
            fg="#10243a",
            font=("Segoe UI", 20, "bold"),
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
        self._queue_item_paths: dict[str, str] = {}
        self._message_item_paths: dict[str, str] = {}

        self.title(APP_TITLE)
        self.geometry("1320x860")
        self.minsize(1100, 720)
        self.configure(bg="#eef2f7")

        self.style = ttk.Style(self)
        self.style.theme_use("clam")
        self.style.configure("Treeview", rowheight=24, font=("Segoe UI", 9))
        self.style.configure("Treeview.Heading", font=("Segoe UI", 9, "bold"))
        self.style.configure("TNotebook", background="#eef2f7")
        self.style.configure("TNotebook.Tab", padding=(10, 6))

        self._build_header()
        self._build_cards()
        self._build_notebook()
        self._build_footer()

        self.after(120, self._present_window)
        self.refresh_now()

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
            screen_w = max(1200, int(self.winfo_screenwidth()))
            screen_h = max(800, int(self.winfo_screenheight()))
            width = min(1320, screen_w - 80)
            height = min(860, screen_h - 80)
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
        header = tk.Frame(self, bg="#0f172a", padx=18, pady=14)
        header.pack(fill="x")

        left = tk.Frame(header, bg="#0f172a")
        left.pack(side="left", fill="x", expand=True)
        tk.Label(
            left,
            text=APP_TITLE,
            bg="#0f172a",
            fg="#f8fafc",
            font=("Segoe UI", 18, "bold"),
        ).pack(anchor="w")
        tk.Label(
            left,
            text="Acompanhe fila, mensagens aguardando original e ultimos processamentos sem usar o terminal.",
            bg="#0f172a",
            fg="#cbd5e1",
            font=("Segoe UI", 9),
        ).pack(anchor="w", pady=(3, 0))

        right = tk.Frame(header, bg="#0f172a")
        right.pack(side="right")
        self.status_label = tk.Label(
            right,
            text="Daemon: -",
            bg="#16324f",
            fg="#f8fafc",
            font=("Segoe UI", 10, "bold"),
            padx=12,
            pady=7,
        )
        self.status_label.pack(side="top", anchor="e")
        actions = tk.Frame(right, bg="#0f172a")
        actions.pack(side="top", anchor="e", pady=(8, 0))
        tk.Button(
            actions,
            text="Atualizar agora",
            command=self.refresh_now,
            bg="#f59e0b",
            fg="#111827",
            activebackground="#fbbf24",
            relief="flat",
            padx=12,
            pady=5,
            font=("Segoe UI", 9, "bold"),
        ).pack(side="left")
        tk.Button(
            actions,
            text="Abrir log",
            command=lambda: self._open_path(self.log_path),
            bg="#1d4ed8",
            fg="#ffffff",
            activebackground="#2563eb",
            relief="flat",
            padx=12,
            pady=5,
            font=("Segoe UI", 9, "bold"),
        ).pack(side="left", padx=(8, 0))

    def _build_cards(self) -> None:
        wrap = tk.Frame(self, bg="#eef2f7", padx=14, pady=12)
        wrap.pack(fill="x")
        self.metric_cards: dict[str, MetricCard] = {}
        titles = [
            "Fila total",
            "Aguard. original",
            "Aguard. ordem",
            "UI pendente",
            "UI rodando",
            "Processando",
            "Excecoes",
            "Receipts 24h",
        ]
        for col, title in enumerate(titles):
            wrap.grid_columnconfigure(col, weight=1)
            card = MetricCard(wrap, title)
            card.grid(row=0, column=col, sticky="nsew", padx=5, pady=4)
            self.metric_cards[title] = card

    def _build_notebook(self) -> None:
        self.notebook = ttk.Notebook(self)
        self.notebook.pack(fill="both", expand=True, padx=14, pady=(0, 12))

        self.queue_tree = self._build_tree_tab(
            "Fila agora",
            (
                ("grupo", 220),
                ("arquivo", 220),
                ("tipo", 130),
                ("fila", 220),
                ("msg_estado", 140),
                ("tentativas", 90),
                ("idade", 90),
                ("proximo", 150),
            ),
        )
        self.queue_tree.bind("<Double-1>", self._open_selected_queue_item)

        self.message_tree = self._build_tree_tab(
            "Mensagens aguardando",
            (
                ("envio", 145),
                ("grupo", 260),
                ("estado", 150),
                ("ui_try", 70),
                ("ultimo_ui", 260),
                ("arquivo", 260),
            ),
        )
        self.message_tree.bind("<Double-1>", self._open_selected_message_item)

        self.receipt_tree = self._build_tree_tab(
            "Ultimos processados",
            (
                ("processado", 145),
                ("cliente", 250),
                ("banco", 140),
                ("valor", 120),
                ("comprovante", 150),
                ("verificacao", 140),
                ("origem", 140),
            ),
        )

        log_frame = tk.Frame(self.notebook, bg="#ffffff")
        self.notebook.add(log_frame, text="Log recente")
        self.log_text = tk.Text(
            log_frame,
            wrap="none",
            bg="#0b1220",
            fg="#dbe4f0",
            insertbackground="#dbe4f0",
            relief="flat",
            font=("Consolas", 10),
        )
        self.log_text.pack(fill="both", expand=True)
        self.log_text.configure(state="disabled")

    def _build_tree_tab(self, title: str, columns: tuple[tuple[str, int], ...]) -> ttk.Treeview:
        frame = tk.Frame(self.notebook, bg="#ffffff")
        self.notebook.add(frame, text=title)

        tree = ttk.Treeview(frame, columns=[name for name, _width in columns], show="headings")
        for name, width in columns:
            tree.heading(name, text=name.replace("_", " ").title())
            tree.column(name, width=width, anchor="w")

        y_scroll = ttk.Scrollbar(frame, orient="vertical", command=tree.yview)
        x_scroll = ttk.Scrollbar(frame, orient="horizontal", command=tree.xview)
        tree.configure(yscrollcommand=y_scroll.set, xscrollcommand=x_scroll.set)

        tree.grid(row=0, column=0, sticky="nsew")
        y_scroll.grid(row=0, column=1, sticky="ns")
        x_scroll.grid(row=1, column=0, sticky="ew")
        frame.grid_rowconfigure(0, weight=1)
        frame.grid_columnconfigure(0, weight=1)
        return tree

    def _build_footer(self) -> None:
        footer = tk.Frame(self, bg="#dfe7f1", padx=14, pady=10)
        footer.pack(fill="x", side="bottom")

        self.last_refresh_label = tk.Label(
            footer,
            text="Ultima atualizacao: -",
            bg="#dfe7f1",
            fg="#203047",
            font=("Segoe UI", 9, "bold"),
        )
        self.last_refresh_label.pack(anchor="w")

        self.summary_label = tk.Label(
            footer,
            text="-",
            bg="#dfe7f1",
            fg="#203047",
            justify="left",
            anchor="w",
            font=("Segoe UI", 9),
        )
        self.summary_label.pack(anchor="w", pady=(6, 0))

        self.error_label = tk.Label(
            footer,
            text="",
            bg="#dfe7f1",
            fg="#b42318",
            justify="left",
            anchor="w",
            font=("Segoe UI", 9, "bold"),
        )
        self.error_label.pack(anchor="w", pady=(6, 0))

    def _replace_tree_rows(
        self,
        tree: ttk.Treeview,
        rows: list[dict[str, str]],
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
                path_map[item_id] = row.get("open_path", "")

    def _render_log(self, lines: list[str]) -> None:
        self.log_text.configure(state="normal")
        self.log_text.delete("1.0", "end")
        text = "\n".join(lines) if lines else "Sem linhas recentes de log."
        self.log_text.insert("1.0", text)
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

    def _open_selected_queue_item(self, _event: Any) -> None:
        selection = self.queue_tree.selection()
        if not selection:
            return
        self._open_path(self._queue_item_paths.get(selection[0], ""))

    def _open_selected_message_item(self, _event: Any) -> None:
        selection = self.message_tree.selection()
        if not selection:
            return
        self._open_path(self._message_item_paths.get(selection[0], ""))

    def refresh_now(self) -> None:
        snapshot = load_snapshot(self.base_dir)

        status_bg = "#166534" if snapshot.daemon_running else "#9f1239"
        self.status_label.configure(text=snapshot.daemon_status, bg=status_bg)
        for title, card in self.metric_cards.items():
            card.set_value(snapshot.metrics.get(title, "-"))

        self._replace_tree_rows(
            self.queue_tree,
            snapshot.queue_rows,
            ["grupo", "arquivo", "tipo", "fila", "msg_estado", "tentativas", "idade", "proximo"],
            self._queue_item_paths,
        )
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

        self.last_refresh_label.configure(text=f"Ultima atualizacao: {fmt_dt(time.time())}")
        summary = (
            f"Ultimo UI: {short_text(snapshot.last_ui_result, 60)} | "
            f"Grupo UI: {short_text(snapshot.last_ui_talker, 40)} | "
            f"Ultima resolucao: {short_text(snapshot.last_resolution, 28)} | "
            f"Ultima verificacao: {short_text(snapshot.last_verification, 24)} | "
            f"Ultima excecao: {short_text(snapshot.last_exception, 42)}"
        )
        self.summary_label.configure(text=summary)
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
