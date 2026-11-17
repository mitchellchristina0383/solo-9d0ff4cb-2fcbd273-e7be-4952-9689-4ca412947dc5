"""End-to-end integration test for alert notification delivery.

Unlike ``selftest.py`` this script starts its **own** receiver and API
processes against a temporary database, which lets it exercise the cases that
need process lifecycle control:

* delivery retries and receiver-side id deduplication,
* per-service ordering while the receiver fails,
* resume after an API restart,
* two API processes (two delivery pools) sharing one database,
* atomicity: a failing notification insert rolls the report back.

Standard library only; run it with the project importable:

    python tests/notification_integration.py

Exit code 0 means every check passed.
"""
from __future__ import annotations

import json
import os
import socket
import sqlite3
import subprocess
import sys
import tempfile
import time
import urllib.error
import urllib.request
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent

PASS = 0
FAIL = 0
FAILURES: list[str] = []


def check(name: str, cond: bool, detail: str = "") -> None:
    global PASS, FAIL
    if cond:
        PASS += 1
        print(f"  PASS  {name}")
    else:
        FAIL += 1
        FAILURES.append(f"{name} {detail}")
        print(f"  FAIL  {name} {detail}")


def free_port() -> int:
    with socket.socket() as s:
        s.bind(("127.0.0.1", 0))
        return s.getsockname()[1]


def call(base: str, method: str, path: str, body: dict | None = None):
    data = json.dumps(body).encode() if body is not None else None
    req = urllib.request.Request(
        base + path, data=data, method=method,
        headers={"Content-Type": "application/json"},
    )
    try:
        with urllib.request.urlopen(req, timeout=20) as resp:
            return resp.status, json.loads(resp.read().decode() or "{}")
    except urllib.error.HTTPError as exc:
        raw = exc.read().decode(errors="replace")
        try:
            return exc.code, json.loads(raw or "{}")
        except ValueError:
            return exc.code, {"raw": raw}


def wait_http(url: str, timeout: float = 20) -> bool:
    deadline = time.time() + timeout
    while time.time() < deadline:
        try:
            with urllib.request.urlopen(url, timeout=1) as resp:
                if resp.status == 200:
                    return True
        except Exception:  # noqa: BLE001
            time.sleep(0.1)
    return False


class Stack:
    def __init__(self) -> None:
        self.tmp = tempfile.TemporaryDirectory()
        self.db = os.path.join(self.tmp.name, "alerts.db")
        self.receiver_port = free_port()
        self.api_procs: list[subprocess.Popen] = []
        self.receiver: subprocess.Popen | None = None
        self.api_ports: list[int] = []
        self.env = dict(os.environ)
        self.env["PYTHONPATH"] = f"{ROOT}{os.pathsep}{ROOT / '.pylibs'}" + (
            os.pathsep + self.env["PYTHONPATH"] if self.env.get("PYTHONPATH") else ""
        )

    # -- lifecycle -------------------------------------------------------
    @property
    def receiver_url(self) -> str:
        return f"http://127.0.0.1:{self.receiver_port}"

    def start_receiver(self) -> None:
        self.receiver = subprocess.Popen(
            [sys.executable, str(ROOT / "receiver" / "server.py")],
            env={**self.env, "RECEIVER_PORT": str(self.receiver_port),
                 "RECEIVER_API_URL": "http://127.0.0.1:1"},
            stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL,
        )
        assert wait_http(self.receiver_url + "/health"), "receiver did not start"

    def stop_receiver(self) -> None:
        if self.receiver:
            self.receiver.terminate()
            self.receiver.wait(timeout=5)
            self.receiver = None

    def start_api(self, *, workers: int = 2) -> int:
        port = free_port()
        env = {
            **self.env,
            "ALERT_DB_PATH": self.db,
            "NOTIFY_RECEIVER_URL": self.receiver_url + "/alerts",
            "NOTIFY_WORKERS": str(workers),
            "NOTIFY_POLL_SECONDS": "0.05",
            "NOTIFY_RETRY_BASE_SECONDS": "0.2",
            "NOTIFY_RETRY_CAP_SECONDS": "0.5",
            "NOTIFY_TIMEOUT_SECONDS": "1",
        }
        proc = subprocess.Popen(
            [sys.executable, "-m", "uvicorn", "app.main:app",
             "--host", "127.0.0.1", "--port", str(port)],
            cwd=str(ROOT), env=env,
            stdout=open(os.path.join(self.tmp.name, f"api-{port}.log"), "wb"),
            stderr=subprocess.STDOUT,
        )
        self.api_procs.append(proc)
        self.api_ports.append(port)
        assert wait_http(f"http://127.0.0.1:{port}/health"), "api did not start"
        return port

    def stop_api(self, index: int = -1) -> None:
        proc = self.api_procs.pop(index)
        port = self.api_ports.pop(index)  # keep indices aligned with procs
        proc.terminate()
        proc.wait(timeout=10)
        print(f"  (stopped API on port {port})")

    def stop_all(self) -> None:
        if os.environ.get("KEEP_TMP"):
            for proc in self.api_procs:
                proc.terminate()
            self.stop_receiver()
            dest = Path("/tmp/notify-test-logs")
            dest.mkdir(exist_ok=True)
            for log in Path(self.tmp.name).glob("api-*.log"):
                (dest / log.name).write_bytes(log.read_bytes())
            print("KEEP_TMP: logs copied to", dest)
            return
        for proc in self.api_procs:
            proc.terminate()
            try:
                proc.wait(timeout=10)
            except subprocess.TimeoutExpired:
                proc.kill()
        self.api_procs.clear()
        self.stop_receiver()
        self.tmp.cleanup()

    # -- helpers ---------------------------------------------------------
    def api(self, index: int = 0) -> str:
        return f"http://127.0.0.1:{self.api_ports[index]}"

    def reset(self) -> None:
        for port in self.api_ports:
            call(f"http://127.0.0.1:{port}", "POST", "/admin/reset")
        call(self.receiver_url, "POST", "/admin/reset")

    def received(self) -> dict:
        return call(self.receiver_url, "GET", "/alerts")[1]

    def outbox(self, index: int = 0) -> dict:
        return call(self.api(index), "GET", "/notifications")[1]

    def report(self, svc: str, seq: int, status: str, index: int = 0):
        return call(self.api(index), "POST", "/reports",
                    {"service": svc, "seq": seq, "status": status})

    def graph(self, services, version="v1", index=0):
        return call(self.api(index), "POST", "/graph",
                    {"version": version, "services": services})

    def wait_received(self, count: int, timeout: float = 10) -> dict:
        deadline = time.time() + timeout
        while time.time() < deadline:
            data = self.received()
            if data["unique_count"] >= count:
                return data
            time.sleep(0.05)
        return self.received()


GRAPH_V1 = [
    {"name": "D", "dependencies": []},
    {"name": "B", "dependencies": ["D"]},
    {"name": "A", "dependencies": ["B"]},
]


def transitions(st: Stack) -> None:
    print("[N1] open / close / new round semantics")
    st.reset()
    st.graph(GRAPH_V1)

    st.report("D", 1, "fault")
    data = st.wait_received(1)
    check("root fault sends one open", data["unique_count"] == 1, str(data))
    rec = data["notifications"][0]
    check("open carries service/action/round",
          rec["service"] == "D" and rec["action"] == "open" and rec["round"] == 1, str(rec))

    st.report("B", 1, "fault")   # suppressed: not pending
    st.report("A", 1, "fault")   # suppressed: not pending
    time.sleep(0.5)
    check("suppressed services produce no notification",
          st.received()["total_posts"] == 1, str(st.received()))

    # Repeated reports while still actionable must not re-send.
    st.report("D", 2, "fault")
    time.sleep(0.5)
    check("still-pending seq bump does not re-notify",
          st.received()["total_posts"] == 1, str(st.received()))

    # D recovers -> B becomes the actionable root.  Only B is notified;
    # D's open is closed.
    st.report("D", 3, "recover")
    data = st.wait_received(3)
    by_service = [(n["service"], n["action"]) for n in data["notifications"]]
    check("D recovery closes D and opens B",
          ("D", "close") in by_service and ("B", "open") in by_service, str(by_service))
    d_close = [n for n in data["notifications"] if n["service"] == "D"][-1]
    check("close reason is 'recovered'", d_close["reason"] == "recovered" and
          d_close["action"] == "close", str(d_close))
    check("same service keeps a monotonic notification seq",
          [n["service_seq"] for n in data["notifications"] if n["service"] == "D"] == [1, 2],
          str(data["notifications"]))

    # B recovers -> A becomes actionable.
    st.report("B", 2, "recover")
    st.wait_received(4)
    # A recovers -> close.
    st.report("A", 2, "recover")
    data = st.wait_received(6)
    check("A close on recovery",
          any(n["service"] == "A" and n["action"] == "close" for n in data["notifications"]),
          str(data["notifications"]))

    # A faults again -> brand new round.
    st.report("A", 3, "fault")
    data = st.wait_received(7)
    a_opens = [n for n in data["notifications"] if n["service"] == "A" and n["action"] == "open"]
    check("re-entry starts a new round", len(a_opens) == 2 and a_opens[-1]["round"] == 2,
          str(a_opens))


def suppression_and_graph(st: Stack) -> None:
    print("[N2] suppression transition and graph replacement notifications")
    st.reset()
    st.graph(GRAPH_V1)
    st.report("D", 1, "fault")
    st.report("B", 1, "fault")
    st.report("A", 1, "fault")
    st.wait_received(1)
    time.sleep(0.4)

    # B was suppressed (never opened), so recovering it sends no close; A's
    # only upstream is now up, so A becomes actionable and is opened.
    st.report("B", 2, "recover")
    data = st.wait_received(2)
    actions = {(n["service"], n["action"], n["reason"]) for n in data["notifications"]}
    check("B recovery opens A without a spurious B notification",
          ("A", "open", "report") in actions and
          not any(n["service"] == "B" for n in data["notifications"]),
          str(actions))

    # Rebuild the same topology to check graph revision label on new opens.
    st.reset()
    st.graph(GRAPH_V1)
    st.report("D", 1, "fault")
    st.report("B", 1, "fault")
    st.wait_received(1)
    time.sleep(0.3)
    # Remove D from the graph: B loses its faulted upstream and becomes a root.
    status, body = st.graph([
        {"name": "B", "dependencies": []},
        {"name": "A", "dependencies": ["B"]},
    ], version="v2")
    check("v2 accepted", status == 200, str(body))
    data = st.wait_received(3)
    d_closes = [n for n in data["notifications"] if n["service"] == "D" and n["action"] == "close"]
    b_opens = [n for n in data["notifications"] if n["service"] == "B" and n["action"] == "open"]
    check("removed service D gets a close notification", len(d_closes) == 1, str(data))
    check("removed-from-graph close reason", d_closes[0]["reason"] == "removed_from_graph",
          str(d_closes[0]))
    check("B becomes actionable after graph change and is opened",
          len(b_opens) == 1 and b_opens[0]["reason"] == "graph_version",
          str(b_opens))

    # A graph change that suppresses a previously actionable service closes it.
    st.reset()
    st.graph([{"name": "B", "dependencies": []}])
    st.report("B", 1, "fault")
    st.wait_received(1)
    time.sleep(0.3)
    st.graph([{"name": "B", "dependencies": ["R"]},
              {"name": "R", "dependencies": []}])
    st.report("R", 1, "fault")   # R becomes actionable root, B now suppressed
    data = st.wait_received(3)
    closes = [n for n in data["notifications"] if n["service"] == "B" and n["action"] == "close"]
    check("graph-induced suppression closes B with reason 'suppressed'",
          len(closes) == 1 and closes[0]["reason"] == "suppressed", str(closes))


def retry_and_dedup(st: Stack) -> None:
    print("[N3] receiver failure -> retry with same id, receiver dedups")
    st.reset()
    st.graph(GRAPH_V1)
    call(st.receiver_url, "POST", "/admin/fail", {"count": 2})

    st.report("D", 1, "fault")
    data = st.wait_received(1, timeout=10)
    check("notification eventually delivered after failures",
          data["unique_count"] == 1, str(data))
    check("receiver saw exactly one unique id", len({n["id"] for n in data["notifications"]}) == 1,
          str(data))
    check("receiver counted retried deliveries as duplicates",
          data["duplicate_deliveries"] == 0 and data["total_posts"] == 3,
          # 3 posts, 2 rejected before storing -> dedup counter only for
          # already-stored ids, so duplicates is 0 here.
          str(data))
    _, outbox = call(st.api(), "GET", "/notifications")
    rec = outbox["notifications"][0]
    check("retry kept the notification id and succeeded on attempt 3",
          rec["status"] == "delivered" and rec["attempts"] == 3, str(rec))

    # Now force a failure *after* the receiver stored the row: the retry must
    # reuse the id and be deduplicated by the receiver.
    st.reset()
    st.graph(GRAPH_V1)
    st.report("D", 1, "fault")
    st.wait_received(1)
    time.sleep(0.3)
    first_id = st.received()["notifications"][0]["id"]
    # Re-deliver the exact same payload to the receiver: it must dedup by id.
    payload = st.received()["notifications"][0]
    clean = {k: v for k, v in payload.items() if k != "received_at"}
    status, body = call(st.receiver_url, "POST", "/alerts", clean)
    check("duplicate id is acknowledged but not stored twice",
          status == 200 and body.get("duplicate") is True
          and st.received()["unique_count"] == 1,
          f"{status} {body}")
    check("dedup counter incremented", st.received()["duplicate_deliveries"] == 1,
          str(st.received()))
    check("same id reused across attempts", first_id == clean["id"])


def ordering(st: Stack) -> None:
    print("[N4] per-service ordering with a stalled head")
    st.reset()
    st.graph(GRAPH_V1)
    st.report("D", 1, "fault")
    st.wait_received(1)
    time.sleep(0.3)

    # Fail the receiver, then produce two events for D rapidly: close then
    # open (a new round).  The close must arrive before the open.
    st.report("D", 2, "recover")
    st.report("D", 3, "fault")
    # Allow enough retries to drain.
    time.sleep(1.5)
    data = st.wait_received(3, timeout=10)
    d_notifications = [n for n in data["notifications"] if n["service"] == "D"]
    check("all three D notifications delivered", len(d_notifications) == 3, str(d_notifications))
    check("service_seq is delivered in order",
          [n["service_seq"] for n in d_notifications] == sorted(n["service_seq"] for n in d_notifications),
          str(d_notifications))
    check("actions ordered open, close, open",
          [n["action"] for n in d_notifications] == ["open", "close", "open"],
          str(d_notifications))


def multi_worker(st: Stack) -> None:
    print("[N5] two API processes / delivery pools do not steal notifications")
    st.reset()
    st.graph(GRAPH_V1)
    # Fan out reports across both API processes; each applied report creates a
    # notification, and both pools race to deliver them.
    st.report("D", 1, "fault", index=0)
    st.report("B", 1, "fault", index=1)   # suppressed (no notification)
    st.report("A", 1, "fault", index=0)   # suppressed (no notification)
    st.report("D", 2, "recover", index=1)
    st.report("B", 2, "fault", index=0)   # B root now
    data = st.wait_received(3, timeout=10)
    time.sleep(0.5)
    data = st.received()
    ids = [n["id"] for n in data["notifications"]]
    check("three unique notifications delivered", data["unique_count"] == 3, str(data))
    check("no id delivered twice by the two pools",
          data["total_posts"] == 3 and data["duplicate_deliveries"] == 0, str(data))
    check("no duplicate storage", len(ids) == len(set(ids)), str(ids))
    out = st.outbox(0)
    check("outbox fully drained", out["pending"] == 0 and out["sending"] == 0
          and out["delivered"] == 3, str(out))


def restart_resume(st: Stack) -> None:
    print("[N6] pending notification resumes after API restart")
    st.reset()
    st.graph(GRAPH_V1)
    st.stop_receiver()
    st.report("D", 1, "fault")
    time.sleep(0.8)
    out = st.outbox()
    check("undeliverable notification stays pending",
          out["pending"] == 1 and out["delivered"] == 0, str(out))

    # Restart the API against the same database, receiver still down.
    st.stop_api(0)
    st.start_api(workers=2)
    time.sleep(0.5)
    out = st.outbox()
    check("pending notification survived the restart", out["pending"] == 1, str(out))
    check("attempts increased while receiver is down",
          out["notifications"][0]["attempts"] >= 1, str(out))

    st.start_receiver()
    data = st.wait_received(1, timeout=15)
    check("notification delivered after receiver returns",
          data["unique_count"] == 1 and data["notifications"][0]["service"] == "D",
          str(data))
    check("outbox drained after restart delivery", st.outbox()["delivered"] == 1,
          str(st.outbox()))


def atomicity(st: Stack) -> None:
    print("[N7] notification write failure rolls the report back")
    st.reset()
    st.graph(GRAPH_V1)
    st.wait_received(0)

    conn = sqlite3.connect(st.db, timeout=10, isolation_level=None)
    conn.execute(
        "CREATE TRIGGER fail_notification BEFORE INSERT ON alert_notifications "
        "BEGIN SELECT RAISE(ABORT, 'injected notification failure'); END"
    )
    conn.close()

    status, body = st.report("D", 1, "fault")
    check("report fails when its notification cannot be written", status == 500,
          f"{status} {body}")

    conn = sqlite3.connect(st.db, timeout=10, isolation_level=None)
    conn.row_factory = sqlite3.Row
    state = conn.execute("SELECT * FROM service_state WHERE service = 'D'").fetchone()
    events = conn.execute("SELECT COUNT(*) AS n FROM events").fetchone()["n"]
    notes = conn.execute("SELECT COUNT(*) AS n FROM alert_notifications").fetchone()["n"]
    conn.close()
    check("alert state was not left behind", state is None, str(dict(state) if state else None))
    check("audit event was rolled back too", events == 0, str(events))
    check("no orphan notification", notes == 0, str(notes))

    conn = sqlite3.connect(st.db, timeout=10, isolation_level=None)
    conn.execute("DROP TRIGGER fail_notification")
    conn.close()

    # The same report now succeeds and notifies exactly once.
    status, body = st.report("D", 1, "fault")
    check("retry after removing the fault succeeds", status == 200 and body["applied"],
          str(body))
    data = st.wait_received(1)
    check("exactly one notification after recovery from write failure",
          data["unique_count"] == 1, str(data))


def startup_upgrade(st: Stack) -> None:
    print("[N8] pre-existing fault state is notified on startup")
    st.reset()
    st.graph(GRAPH_V1)
    st.report("D", 1, "fault")
    st.wait_received(1)
    time.sleep(0.3)

    # Simulate a database from before the outbox feature: alert state exists
    # but no notification bookkeeping.  On startup the API must re-derive it.
    conn = sqlite3.connect(st.db, timeout=10, isolation_level=None)
    conn.execute("DELETE FROM alert_notifications")
    conn.execute("DELETE FROM notification_state")
    conn.close()
    call(st.receiver_url, "POST", "/admin/reset")

    st.stop_api(0)
    st.start_api(workers=2)
    data = st.wait_received(1, timeout=10)
    check("startup reconciliation notifies existing actionable fault",
          data["unique_count"] == 1 and data["notifications"][0]["service"] == "D"
          and data["notifications"][0]["action"] == "open", str(data))
    check("startup open is round 1", data["notifications"][0]["round"] == 1, str(data))

    # A second restart must not re-notify a still-pending service.
    st.stop_api(0)
    st.start_api(workers=2)
    time.sleep(0.8)
    check("restart does not duplicate an already-open notification",
          st.received()["unique_count"] == 1, str(st.received()))


def main() -> int:
    st = Stack()
    try:
        st.start_receiver()
        st.start_api(workers=3)
        # A second API process shares the same database and its own pool.
        st.start_api(workers=3)

        transitions(st)
        suppression_and_graph(st)
        retry_and_dedup(st)
        ordering(st)
        multi_worker(st)
        restart_resume(st)
        atomicity(st)
        startup_upgrade(st)
    finally:
        st.stop_all()

    print(f"\n{'-' * 60}\nPASS={PASS} FAIL={FAIL}")
    if FAIL:
        print("Failures:")
        for f in FAILURES:
            print(" -", f)
        return 1
    print("ALL NOTIFICATION CHECKS PASSED")
    return 0


if __name__ == "__main__":
    sys.exit(main())
