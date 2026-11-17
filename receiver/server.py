"""Local alert receiver ("接警端") used by docker compose.

It is deliberately small and dependency-free (standard library only):

* ``POST /alerts``  accept one open/close notification, deduplicated by id;
* ``GET  /alerts``  JSON view of stored notifications and duplicate counters;
* ``GET  /docs``    human-readable page showing received records;
* ``GET  /health``  liveness for compose health checks;
* ``POST /admin/reset`` clear state (test helper).

Duplicate handling is id-based: the first delivery of an id is stored, later
deliveries of the same id are acknowledged with ``200`` but only bump a
``duplicates`` counter.  That is what makes delivery retries safe.
"""
from __future__ import annotations

import json
import os
import threading
import time
import urllib.request
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from urllib.parse import urlparse

HOST = os.environ.get("RECEIVER_HOST", "0.0.0.0")
PORT = int(os.environ.get("RECEIVER_PORT", "9000"))
# Optional: the API outbox is shown on /docs so operators can see, next to the
# received records, which notifications the API has not delivered yet.
API_URL = os.environ.get("RECEIVER_API_URL", "http://alert-api:8000")

_lock = threading.Lock()
_seen: dict[str, dict] = {}          # id -> stored record
_order: list[str] = []               # arrival order of unique ids
_duplicates: dict[str, int] = {}     # id -> number of repeat deliveries
_total_posts = 0
_fail_next = 0                       # test hook: fail the next N POSTs


def _reset() -> None:
    global _total_posts, _fail_next
    with _lock:
        _seen.clear()
        _order.clear()
        _duplicates.clear()
        _total_posts = 0
        _fail_next = 0


DOCS_HTML = """<!doctype html>
<html lang="zh">
<head>
<meta charset="utf-8">
<title>Alert Receiver</title>
<style>
  body { font-family: system-ui, sans-serif; margin: 2rem; color: #1a202c; }
  h1 { font-size: 1.4rem; } h2 { font-size: 1.1rem; margin-top: 2rem; }
  table { border-collapse: collapse; width: 100%; }
  th, td { border-bottom: 1px solid #e2e8f0; padding: .45rem .6rem;
           text-align: left; font-size: .9rem; vertical-align: top; }
  th { background: #f7fafc; position: sticky; top: 0; }
  .open { color: #c53030; font-weight: 600; }
  .close { color: #2f855a; font-weight: 600; }
  .muted { color: #718096; }
  .badge { display: inline-block; padding: .1rem .45rem; border-radius: 4px;
           font-size: .78rem; font-weight: 600; }
  .pending { background: #fefcbf; color: #744210; }
  .sending { background: #bee3f8; color: #2a4365; }
  .delivered { background: #c6f6d5; color: #22543d; }
  code { background: #edf2f7; padding: .1rem .3rem; border-radius: 3px; }
  .unavailable { color: #c53030; }
</style>
</head>
<body>
  <h1>Alert Receiver — 接警记录</h1>
  <p class="muted">
    数据来源 <code>GET /alerts</code>；同一通知 ID 只记录一次（重试去重）。
    共 <span id="total">-</span> 条，重复投递 <span id="dups">-</span> 次。
    页面每 2 秒刷新。
  </p>
  <table>
    <thead><tr>
      <th>收到时间</th><th>类型</th><th>服务</th><th>通知 ID</th>
      <th>服务序号</th><th>轮次</th><th>原因</th><th>图版本</th><th>重复</th>
    </tr></thead>
    <tbody id="rows"></tbody>
  </table>

  <h2>API 待发通知（未送达）</h2>
  <p class="muted">
    来自 API 发件箱 <code>GET /notifications</code>：
    待发 <span id="p-pending">-</span>，投递中 <span id="p-sending">-</span>，
    已送达 <span id="p-delivered">-</span>。
    <span id="p-error" class="unavailable"></span>
  </p>
  <table>
    <thead><tr>
      <th>服务</th><th>类型</th><th>通知 ID</th><th>服务序号</th>
      <th>状态</th><th>尝试次数</th><th>最近错误</th>
    </tr></thead>
    <tbody id="pending-rows"></tbody>
  </table>
<script>
async function refreshReceived() {
  const r = await fetch('/alerts');
  const d = await r.json();
  document.getElementById('total').textContent = d.unique_count;
  document.getElementById('dups').textContent = d.duplicate_deliveries;
  const rows = d.notifications.map(n => {
    const dup = d.duplicates[n.id] || 0;
    return `<tr>
      <td class="muted">${n.received_at}</td>
      <td class="${n.action}">${n.action}</td>
      <td>${n.service}</td>
      <td><code>${n.id}</code></td>
      <td>${n.service_seq}</td>
      <td>${n.round}</td>
      <td>${n.reason || ''}</td>
      <td>${n.graph_version ?? ''}</td>
      <td>${dup || ''}</td>
    </tr>`;
  }).reverse().join('');
  document.getElementById('rows').innerHTML = rows;
}
async function refreshPending() {
  const err = document.getElementById('p-error');
  try {
    const r = await fetch('/upstream/notifications');
    const d = await r.json();
    if (!r.ok) throw new Error(d.detail || ('HTTP ' + r.status));
    document.getElementById('p-pending').textContent = d.pending;
    document.getElementById('p-sending').textContent = d.sending;
    document.getElementById('p-delivered').textContent = d.delivered;
    err.textContent = '';
    const rows = d.notifications
      .filter(n => n.status !== 'delivered' || d.notifications.length <= 50)
      .map(n => `<tr>
        <td>${n.service}</td>
        <td class="${n.action}">${n.action}</td>
        <td><code>${n.id}</code></td>
        <td>${n.service_seq}</td>
        <td><span class="badge ${n.status}">${n.status}</span></td>
        <td>${n.attempts}</td>
        <td class="muted">${n.last_error || ''}</td>
      </tr>`).join('');
    document.getElementById('pending-rows').innerHTML = rows;
  } catch (e) {
    err.textContent = 'API 不可用：' + e.message;
  }
}
refreshReceived(); refreshPending();
setInterval(refreshReceived, 2000);
setInterval(refreshPending, 2000);
</script>
</body>
</html>
"""


class Handler(BaseHTTPRequestHandler):
    protocol_version = "HTTP/1.1"

    # -- helpers ---------------------------------------------------------
    def _send(self, status: int, body: bytes, content_type: str) -> None:
        self.send_response(status)
        self.send_header("Content-Type", content_type)
        self.send_header("Content-Length", str(len(body)))
        self.end_headers()
        self.wfile.write(body)

    def _json(self, status: int, payload: dict) -> None:
        self._send(status, json.dumps(payload).encode(), "application/json")

    def _read_json(self) -> dict | None:
        length = int(self.headers.get("Content-Length", 0) or 0)
        raw = self.rfile.read(length) if length else b""
        try:
            return json.loads(raw.decode() or "{}")
        except (ValueError, UnicodeDecodeError):
            return None

    def log_message(self, fmt, *args):  # noqa: D401 - quieter default logging
        print(f"receiver: {fmt % args}", flush=True)

    # -- routes ----------------------------------------------------------
    def do_GET(self) -> None:  # noqa: N802
        path = urlparse(self.path).path
        if path in ("/docs", "/"):
            self._send(200, DOCS_HTML.encode(), "text/html; charset=utf-8")
        elif path == "/health":
            self._json(200, {"status": "ok"})
        elif path == "/upstream/notifications":
            # Server-side proxy so the /docs page can show API outbox state
            # without relying on browser CORS.
            try:
                req = urllib.request.Request(
                    API_URL.rstrip("/") + "/notifications", method="GET"
                )
                with urllib.request.urlopen(req, timeout=3) as resp:
                    self._send(200, resp.read(), "application/json")
            except Exception as exc:  # noqa: BLE001
                self._json(502, {"detail": f"API unavailable: {exc}"})
        elif path == "/alerts":
            with _lock:
                records = [_seen[i] for i in _order]
                body = {
                    "unique_count": len(records),
                    "total_posts": _total_posts,
                    "duplicate_deliveries": sum(_duplicates.values()),
                    "duplicates": dict(_duplicates),
                    "notifications": records,
                }
            self._json(200, body)
        else:
            self._json(404, {"error": "not_found"})

    def do_POST(self) -> None:  # noqa: N802
        global _total_posts, _fail_next
        path = urlparse(self.path).path
        if path == "/admin/reset":
            _reset()
            self._json(200, {"reset": True})
            return
        if path == "/admin/fail":
            # Test hook: {"count": n} makes the next n POST /alerts fail with 503.
            data = self._read_json() or {}
            with _lock:
                _fail_next = int(data.get("count", 1))
            self._json(200, {"fail_next": _fail_next})
            return
        if path != "/alerts":
            self._json(404, {"error": "not_found"})
            return

        data = self._read_json()
        if not isinstance(data, dict) or not data.get("id") or not data.get("service"):
            self._json(422, {"error": "invalid_notification"})
            return
        with _lock:
            _total_posts += 1
            if _fail_next > 0:
                _fail_next -= 1
                failed = True
            else:
                failed = False
        if failed:
            self._json(503, {"error": "injected_failure"})
            return

        nid = str(data["id"])
        with _lock:
            if nid in _seen:
                _duplicates[nid] = _duplicates.get(nid, 0) + 1
                duplicate = True
            else:
                data = dict(data)
                data["received_at"] = time.strftime(
                    "%Y-%m-%dT%H:%M:%SZ", time.gmtime()
                )
                _seen[nid] = data
                _order.append(nid)
                duplicate = False
        self._json(200, {"ok": True, "duplicate": duplicate})


def main() -> None:
    server = ThreadingHTTPServer((HOST, PORT), Handler)
    server.daemon_threads = True
    print(f"receiver: listening on {HOST}:{PORT}", flush=True)
    server.serve_forever()


if __name__ == "__main__":
    main()
