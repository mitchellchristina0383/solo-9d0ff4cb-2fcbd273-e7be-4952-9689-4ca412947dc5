"""Background delivery loop that drains the notification outbox.

Design notes
------------
* Delivery happens in a separate thread, never inside the report/graph
  transaction, so a slow or failing receiver can never roll back alert state.
* A row is *leased* (``status='sending'``) under a ``BEGIN IMMEDIATE``
  transaction before any network I/O.  The lease is what stops two workers
  from sending the same notification; if a worker dies mid-send the lease
  expires and the row becomes claimable again.
* The HTTP POST is made outside any transaction.  On success a short
  transaction marks the row delivered; on failure it goes back to ``pending``
  with exponential backoff.  The notification ``id`` is stable across retries.
* Because the claim query skips a row while an earlier row for the same
  service is undelivered, notifications for one service are delivered in
  ``service_seq`` order -- one slow head blocks only that service.
"""
from __future__ import annotations

import json
import logging
import os
import socket
import threading
import time
import urllib.request
import uuid

from .database import DB_PATH, close_connection, write_tx
from .notifications import (
    backoff_delay,
    claim_notification,
    mark_delivered,
    mark_retry,
)

log = logging.getLogger("alert.notifications")

RECEIVER_URL = os.environ.get("NOTIFY_RECEIVER_URL", "http://notify-receiver:9000/alerts")
REQUEST_TIMEOUT = float(os.environ.get("NOTIFY_TIMEOUT_SECONDS", "3"))
RETRY_BASE = float(os.environ.get("NOTIFY_RETRY_BASE_SECONDS", "0.5"))
RETRY_CAP = float(os.environ.get("NOTIFY_RETRY_CAP_SECONDS", "10"))
LEASE_SECONDS = float(os.environ.get("NOTIFY_LEASE_SECONDS", "30"))
POLL_INTERVAL = float(os.environ.get("NOTIFY_POLL_SECONDS", "0.25"))
WORKERS = max(1, int(os.environ.get("NOTIFY_WORKERS", "2")))


def deliver(payload: dict) -> None:
    """POST one notification.  Raise on any non-2xx / transport problem."""
    body = json.dumps(payload).encode()
    req = urllib.request.Request(
        RECEIVER_URL,
        data=body,
        method="POST",
        headers={"Content-Type": "application/json"},
    )
    with urllib.request.urlopen(req, timeout=REQUEST_TIMEOUT) as resp:
        if not 200 <= resp.status < 300:
            raise RuntimeError(f"receiver returned HTTP {resp.status}")


def _deliver_once(owner: str) -> bool:
    """Claim and attempt one notification.  Returns True if a row was handled."""
    now = time.time()
    with write_tx() as conn:
        row = claim_notification(
            conn, owner=owner, now=now, lease_seconds=LEASE_SECONDS
        )
        if row is None:
            return False
        notification = {k: row[k] for k in row.keys()}
        payload = json.loads(notification["payload"])

    try:
        deliver(payload)
    except Exception as exc:  # noqa: BLE001 - any send problem must be retried
        delay = backoff_delay(int(notification["attempts"]), RETRY_BASE, RETRY_CAP)
        with write_tx() as conn:
            mark_retry(
                conn, notification["id"], error=f"{type(exc).__name__}: {exc}", delay=delay
            )
        log.warning(
            "notification %s (%s %s) attempt %s failed: %s; retry in %.2fs",
            notification["id"], notification["action"], notification["service"],
            notification["attempts"], exc, delay,
        )
        return True

    with write_tx() as conn:
        mark_delivered(conn, notification["id"])
    log.info(
        "notification %s (%s %s) delivered",
        notification["id"], notification["action"], notification["service"],
    )
    return True


def _worker(stop: threading.Event, worker_id: int) -> None:
    owner = f"{socket.gethostname()}:{os.getpid()}:{worker_id}:{uuid.uuid4().hex[:8]}"
    while not stop.is_set():
        try:
            handled = _deliver_once(owner)
        except Exception:  # noqa: BLE001 - a worker must never die silently
            log.exception("notification worker %s crashed on an iteration", owner)
            handled = False
        if not handled:
            stop.wait(POLL_INTERVAL)
    close_connection()


def start_workers() -> tuple[threading.Event, list[threading.Thread]]:
    """Start the background delivery pool.  Returns ``(stop_event, threads)``."""
    stop = threading.Event()
    threads = [
        threading.Thread(target=_worker, args=(stop, i), name=f"notify-{i}", daemon=True)
        for i in range(WORKERS)
    ]
    for t in threads:
        t.start()
    log.info(
        "notification delivery started: %d worker(s) -> %s (db=%s)",
        WORKERS, RECEIVER_URL, DB_PATH,
    )
    return stop, threads


def stop_workers(stop: threading.Event, threads: list[threading.Thread]) -> None:
    stop.set()
    for t in threads:
        t.join(timeout=REQUEST_TIMEOUT + 2)
