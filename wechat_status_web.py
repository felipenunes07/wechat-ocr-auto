from __future__ import annotations

import argparse
import json
import threading
import webbrowser
from http import HTTPStatus
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path
from typing import Any
from urllib.request import urlopen

from wechat_status_dashboard import load_snapshot


HOST = "127.0.0.1"
DEFAULT_PORT = 8765


HTML_PAGE = """<!doctype html>
<html lang="pt-BR">
<head>
  <meta charset="utf-8">
  <meta name="viewport" content="width=device-width, initial-scale=1">
  <title>Painel WeChat OCR</title>
  <style>
    :root {
      --bg: #edf2f7;
      --ink: #10243a;
      --muted: #526377;
      --card: #ffffff;
      --line: #d7e0ea;
      --good: #166534;
      --bad: #9f1239;
      --accent: #d97706;
      --accent-2: #1d4ed8;
      --log: #0b1220;
    }
    * { box-sizing: border-box; }
    body {
      margin: 0;
      font-family: "Segoe UI", Tahoma, sans-serif;
      background: linear-gradient(180deg, #eff4fb 0%, #edf2f7 100%);
      color: var(--ink);
    }
    header {
      background: #0f172a;
      color: #f8fafc;
      padding: 18px 22px;
      position: sticky;
      top: 0;
      z-index: 2;
      box-shadow: 0 8px 24px rgba(15, 23, 42, 0.18);
    }
    .header-row {
      display: flex;
      justify-content: space-between;
      gap: 16px;
      align-items: center;
      flex-wrap: wrap;
    }
    .title {
      font-size: 28px;
      font-weight: 700;
      margin: 0;
    }
    .subtitle {
      color: #cbd5e1;
      margin-top: 6px;
      font-size: 14px;
    }
    .status-pill {
      padding: 10px 14px;
      border-radius: 999px;
      font-weight: 700;
      background: #16324f;
    }
    .toolbar {
      margin-top: 14px;
      display: flex;
      gap: 10px;
      flex-wrap: wrap;
    }
    button {
      border: 0;
      border-radius: 10px;
      padding: 10px 14px;
      cursor: pointer;
      font-weight: 700;
    }
    .btn-refresh { background: #f59e0b; color: #111827; }
    .btn-log { background: #1d4ed8; color: #fff; }
    main {
      padding: 16px 18px 28px;
      max-width: 1600px;
      margin: 0 auto;
    }
    .cards {
      display: grid;
      grid-template-columns: repeat(auto-fit, minmax(155px, 1fr));
      gap: 10px;
      margin-bottom: 14px;
    }
    .card {
      background: var(--card);
      border: 1px solid var(--line);
      border-radius: 16px;
      padding: 12px 14px;
      box-shadow: 0 8px 20px rgba(16, 36, 58, 0.05);
    }
    .card h3 {
      margin: 0;
      font-size: 13px;
      color: var(--muted);
      font-weight: 700;
    }
    .card .value {
      font-size: 30px;
      font-weight: 800;
      margin-top: 8px;
    }
    .panel-grid {
      display: grid;
      grid-template-columns: 1.2fr 1fr;
      gap: 14px;
    }
    .panel {
      background: var(--card);
      border: 1px solid var(--line);
      border-radius: 18px;
      padding: 14px;
      box-shadow: 0 10px 20px rgba(16, 36, 58, 0.05);
      overflow: hidden;
    }
    .panel h2 {
      margin: 0 0 10px;
      font-size: 18px;
    }
    .summary {
      color: var(--muted);
      font-size: 14px;
      margin-bottom: 12px;
      line-height: 1.45;
    }
    .table-wrap {
      overflow: auto;
      max-height: 340px;
      border: 1px solid var(--line);
      border-radius: 12px;
    }
    table {
      width: 100%;
      border-collapse: collapse;
      font-size: 13px;
      background: white;
    }
    th, td {
      padding: 9px 10px;
      border-bottom: 1px solid #e8edf3;
      text-align: left;
      white-space: nowrap;
    }
    th {
      position: sticky;
      top: 0;
      background: #f8fafc;
      z-index: 1;
    }
    .wide { white-space: normal; min-width: 200px; }
    .log-box {
      background: var(--log);
      color: #dbe4f0;
      border-radius: 14px;
      padding: 14px;
      min-height: 220px;
      max-height: 320px;
      overflow: auto;
      white-space: pre-wrap;
      font-family: Consolas, monospace;
      font-size: 12px;
    }
    .footer-note {
      margin-top: 12px;
      color: var(--muted);
      font-size: 13px;
    }
    .error {
      margin-top: 10px;
      color: var(--bad);
      font-weight: 700;
    }
    @media (max-width: 980px) {
      .panel-grid {
        grid-template-columns: 1fr;
      }
    }
  </style>
</head>
<body>
  <header>
    <div class="header-row">
      <div>
        <h1 class="title">Painel WeChat OCR</h1>
        <div class="subtitle">Fila, espera de imagem original, ordem de envio e ultimos processamentos sem terminal.</div>
      </div>
      <div id="daemonStatus" class="status-pill">Carregando...</div>
    </div>
    <div class="toolbar">
      <button class="btn-refresh" onclick="refreshNow()">Atualizar agora</button>
      <button class="btn-log" onclick="window.scrollTo({top: document.body.scrollHeight, behavior: 'smooth'})">Ir para log</button>
    </div>
  </header>
  <main>
    <section class="cards" id="cards"></section>
    <section class="panel-grid">
      <div class="panel">
        <h2>Fila agora</h2>
        <div class="summary">Itens aguardando imagem melhor, ordem anterior ou processamento.</div>
        <div class="table-wrap"><table id="queueTable"></table></div>
      </div>
      <div class="panel">
        <h2>Mensagens aguardando</h2>
        <div class="summary">Mensagens ainda nao resolvidas, ordenadas pela hora de envio.</div>
        <div class="table-wrap"><table id="messageTable"></table></div>
      </div>
      <div class="panel">
        <h2>Ultimos processados</h2>
        <div class="summary" id="summaryLine">Carregando...</div>
        <div class="table-wrap"><table id="receiptTable"></table></div>
      </div>
      <div class="panel">
        <h2>Log recente</h2>
        <div id="errorBox" class="error"></div>
        <div class="log-box" id="logBox">Carregando log...</div>
        <div class="footer-note" id="refreshLine">Atualizacao automatica a cada 5 segundos.</div>
      </div>
    </section>
  </main>
  <script>
    const cardOrder = [
      "Fila total",
      "Aguard. original",
      "Aguard. ordem",
      "UI pendente",
      "UI rodando",
      "Processando",
      "Excecoes",
      "Receipts 24h"
    ];

    function renderCards(metrics) {
      const root = document.getElementById("cards");
      root.innerHTML = "";
      for (const key of cardOrder) {
        const el = document.createElement("div");
        el.className = "card";
        el.innerHTML = `<h3>${key}</h3><div class="value">${metrics[key] ?? "-"}</div>`;
        root.appendChild(el);
      }
    }

    function renderTable(id, columns, rows) {
      const table = document.getElementById(id);
      const thead = `<thead><tr>${columns.map(col => `<th>${col.label}</th>`).join("")}</tr></thead>`;
      const bodyRows = rows.length
        ? rows.map(row => `<tr>${columns.map(col => `<td class="${col.wide ? "wide" : ""}">${escapeHtml(row[col.key] ?? "-")}</td>`).join("")}</tr>`).join("")
        : `<tr><td colspan="${columns.length}">Sem itens no momento.</td></tr>`;
      table.innerHTML = `${thead}<tbody>${bodyRows}</tbody>`;
    }

    function escapeHtml(value) {
      return String(value)
        .replaceAll("&", "&amp;")
        .replaceAll("<", "&lt;")
        .replaceAll(">", "&gt;");
    }

    async function refreshNow() {
      try {
        const response = await fetch("/api/status?_=" + Date.now(), { cache: "no-store" });
        const data = await response.json();
        document.getElementById("daemonStatus").textContent = data.daemon_status;
        document.getElementById("daemonStatus").style.background = data.daemon_running ? "#166534" : "#9f1239";
        renderCards(data.metrics || {});
        renderTable("queueTable", [
          { key: "grupo", label: "Grupo" },
          { key: "arquivo", label: "Arquivo" },
          { key: "tipo", label: "Tipo" },
          { key: "fila", label: "Fila", wide: true },
          { key: "msg_estado", label: "Estado msg" },
          { key: "tentativas", label: "Tentativas" },
          { key: "idade", label: "Idade" },
          { key: "proximo", label: "Proximo" }
        ], data.queue_rows || []);
        renderTable("messageTable", [
          { key: "envio", label: "Envio" },
          { key: "grupo", label: "Grupo", wide: true },
          { key: "estado", label: "Estado" },
          { key: "ui_try", label: "UI try" },
          { key: "ultimo_ui", label: "Ultimo UI", wide: true },
          { key: "arquivo", label: "Arquivo", wide: true }
        ], data.message_rows || []);
        renderTable("receiptTable", [
          { key: "processado", label: "Processado" },
          { key: "cliente", label: "Cliente", wide: true },
          { key: "banco", label: "Banco" },
          { key: "valor", label: "Valor" },
          { key: "comprovante", label: "Comprovante" },
          { key: "verificacao", label: "Verificacao" },
          { key: "origem", label: "Origem" }
        ], data.receipt_rows || []);
        document.getElementById("summaryLine").textContent =
          `Ultimo UI: ${data.last_ui_result || "-"} | Grupo UI: ${data.last_ui_talker || "-"} | ` +
          `Ultima resolucao: ${data.last_resolution || "-"} | Ultima verificacao: ${data.last_verification || "-"} | ` +
          `Ultima excecao: ${data.last_exception || "-"}`;
        document.getElementById("logBox").textContent = (data.log_lines || []).join("\\n") || "Sem log recente.";
        document.getElementById("refreshLine").textContent = `Atualizacao automatica a cada 5 segundos. Ultima leitura: ${data.refreshed_at || "-"}`;
        document.getElementById("errorBox").textContent = data.error ? `Erro de leitura: ${data.error}` : "";
      } catch (error) {
        document.getElementById("errorBox").textContent = "Falha ao atualizar o painel: " + error;
      }
    }

    refreshNow();
    setInterval(refreshNow, 5000);
  </script>
</body>
</html>
"""


def snapshot_to_dict(base_dir: Path) -> dict[str, Any]:
    snap = load_snapshot(base_dir)
    return {
        "daemon_status": snap.daemon_status,
        "daemon_running": snap.daemon_running,
        "metrics": snap.metrics,
        "queue_rows": snap.queue_rows,
        "message_rows": snap.message_rows,
        "receipt_rows": snap.receipt_rows,
        "log_lines": snap.log_lines,
        "last_ui_result": snap.last_ui_result,
        "last_ui_talker": snap.last_ui_talker,
        "last_exception": snap.last_exception,
        "last_resolution": snap.last_resolution,
        "last_verification": snap.last_verification,
        "error": snap.error,
        "refreshed_at": __import__("datetime").datetime.now().strftime("%d/%m %H:%M:%S"),
    }


class StatusHandler(BaseHTTPRequestHandler):
    base_dir = Path(__file__).resolve().parent

    def _send_bytes(self, content: bytes, content_type: str, status: int = HTTPStatus.OK) -> None:
        self.send_response(status)
        self.send_header("Content-Type", content_type)
        self.send_header("Content-Length", str(len(content)))
        self.send_header("Cache-Control", "no-store")
        self.end_headers()
        self.wfile.write(content)

    def do_GET(self) -> None:
        path = self.path.split("?", 1)[0]
        if path == "/health":
            self._send_bytes(b"ok", "text/plain; charset=utf-8")
            return
        if path == "/api/status":
            payload = json.dumps(snapshot_to_dict(self.base_dir), ensure_ascii=False).encode("utf-8")
            self._send_bytes(payload, "application/json; charset=utf-8")
            return
        if path == "/" or path == "/index.html":
            self._send_bytes(HTML_PAGE.encode("utf-8"), "text/html; charset=utf-8")
            return
        self._send_bytes(b"not found", "text/plain; charset=utf-8", status=HTTPStatus.NOT_FOUND)

    def log_message(self, _format: str, *_args: Any) -> None:
        return


def server_alive(port: int) -> bool:
    try:
        with urlopen(f"http://{HOST}:{port}/health", timeout=1.5) as response:
            return response.status == 200
    except Exception:
        return False


def open_browser_later(url: str) -> None:
    threading.Timer(0.8, lambda: webbrowser.open(url)).start()


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--port", type=int, default=DEFAULT_PORT)
    parser.add_argument("--no-open-browser", action="store_true")
    args = parser.parse_args()

    url = f"http://{HOST}:{int(args.port)}/"
    if server_alive(int(args.port)):
        if not args.no_open_browser:
            webbrowser.open(url)
        print(f"SERVER_REUSED {url}")
        return 0

    server = ThreadingHTTPServer((HOST, int(args.port)), StatusHandler)
    if not args.no_open_browser:
        open_browser_later(url)
    print(f"SERVER_STARTED {url}")
    try:
        server.serve_forever(poll_interval=0.5)
    except KeyboardInterrupt:
        pass
    finally:
        server.server_close()
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
