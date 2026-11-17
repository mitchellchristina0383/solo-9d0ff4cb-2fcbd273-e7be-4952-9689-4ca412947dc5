"""Transactional outbox for alert open/close notifications.

A notification is *not* sent as part of the request that changes alert state.
Instead the report/graph transaction also writes the notification row; a
separate delivery loop drains the outbox.  This gives the two properties the
scheduler needs:

* the alert change and its notification become visible atomically -- if the
  notification insert fails, the whole transaction is rolled back, so there is
  never an alert without its notification;
* delivery failures never touch the alert state -- the outbox row is simply
  retried later.

Per-service ``service_seq`` is monotonic and the delivery query refuses to
hand out a row while an earlier row for the same service is still undelivered,
which preserves notification order for every service.
"""
from __future__ import annotations

import json
import sqlite3
import time
import uuid

OPEN = "open"
CLOSE = "close"

# Reasons carried on the notification for operators/receiver logs.
REASON_REPORT = "report"
REASON_GRAPH = "graph_version"
REASON_RECOVERED = "recovered"
REASON_SUPPRESSED = "suppressed"
REASON_REMOVED = "removed_from_graph"


def _make_payload(
    *,
    notification_id: str,
    service: str,
    action: str,
    service_seq: int,
    round_no: int,
    reason: str,
    snapshot: dict,
    created_at: float,
) -> str:
    return json.dumps(
        {
            "id": notification_id,
            "service": service,
            "action": action,
            "service_seq": service_seq,
            "round": round_no,
            "reason": reason,
            "graph_revision": snapshot["revision"],
            "graph_version": snapshot["version"],
            "created_at": created_at,
        },
        separators=(",", ":"),
        sort_keys=True,
    )


def _close_reason(service: str, in_graph: bool, down: set[str]) -> str:
    if not in_graph:
        return REASON_REMOVED
    if service in down:
        # Still faulty but no longer a root cause: it is now suppressed.
        return REASON_SUPPRESSED
    return REASON_RECOVERED


def reconcile_notifications(conn: sqlite3.Connection, *, trigger: str) -> None:
    """Emit open/close rows for every service whose pending membership changed.

    Must be called inside the *same* write transaction as the report or graph
    replacement.  ``trigger`` is ``report`` or ``graph_version``; it is only
    used as the reason on an ``open`` row (closes carry their own reason).
    """
    # Imported lazily: graph_service calls this function, so a module-level
    # import here would create a cycle.
    from .graph_service import classify_snapshot, load_snapshot

    snapshot = load_snapshot(conn)
    classification = classify_snapshot(snapshot)
    actionable = {f["service"] for f in classification["actionable_faults"]}
    in_graph = set(snapshot["upstream"])
    down = set(snapshot["down"])

    states = {
        row["service"]: row
        for row in conn.execute(
            "SELECT service, last_seq, round, is_pending FROM notification_state"
        )
    }

    now = time.time()
    for service in sorted(set(states) | in_graph):
        st = states.get(service)
        was_pending = bool(st["is_pending"]) if st is not None else False
        last_seq = int(st["last_seq"]) if st is not None else 0
        round_no = int(st["round"]) if st is not None else 0

        desired = service in in_graph and service in actionable
        if desired == was_pending:
            continue

        service_seq = last_seq + 1
        if desired:
            action = OPEN
            reason = trigger
            round_no += 1
        else:
            action = CLOSE
            reason = _close_reason(service, service in in_graph, down)

        notification_id = uuid.uuid4().hex
        conn.execute(
            "INSERT INTO notification_state (service, last_seq, round, is_pending, updated_at) "
            "VALUES (?, ?, ?, ?, strftime('%Y-%m-%dT%H:%M:%fZ', 'now')) "
            "ON CONFLICT(service) DO UPDATE SET last_seq = excluded.last_seq, "
            "round = excluded.round, is_pending = excluded.is_pending, "
            "updated_at = excluded.updated_at",
            (service, service_seq, round_no, int(desired)),
        )
        conn.execute(
            "INSERT INTO alert_notifications "
            "(id, service, action, service_seq, round, reason, payload) "
            "VALUES (?, ?, ?, ?, ?, ?, ?)",
            (
                notification_id,
                service,
                action,
                service_seq,
                round_no,
                reason,
                _make_payload(
                    notification_id=notification_id,
                    service=service,
                    action=action,
                    service_seq=service_seq,
                    round_no=round_no,
                    reason=reason,
                    snapshot=snapshot,
                    created_at=now,
                ),
            ),
        )


def reconcile_startup() -> None:
    """Bring notification bookkeeping in line with the committed state.

    Normally notification rows are written atomically with the change that
    caused them, so this is a no-op.  It matters when the outbox feature is
    enabled on a database that already has fault state: currently actionable
    services have no ``notification_state`` row yet, and they should be sent
    as a new round rather than silently skipped.
    """
    from .database import write_tx
    from .errors import NoGraph

    try:
        with write_tx() as conn:
            # ``trigger`` is only used for opens; use the graph label since
            # that is what is being reconciled at startup.
            reconcile_notifications(conn, trigger=REASON_GRAPH)
    except NoGraph:
        return


# ---------------------------------------------------------------------------
# Outbox reads
# ---------------------------------------------------------------------------


def _notification_dict(row: sqlite3.Row) -> dict:
    return {
        "id": row["id"],
        "service": row["service"],
        "action": row["action"],
        "service_seq": row["service_seq"],
        "round": row["round"],
        "reason": row["reason"],
        "payload": json.loads(row["payload"]),
        "status": row["status"],
        "attempts": row["attempts"],
        "next_attempt_at": row["next_attempt_at"],
        "last_error": row["last_error"],
        "created_at": row["created_at"],
        "delivered_at": row["delivered_at"],
    }


def list_notifications(
    filter_status: str | None = None, limit: int = 200
) -> dict:
    """Return outbox rows (oldest first) plus a status summary."""
    from .database import read_tx

    where = ""
    params: list[object] = []
    if filter_status:
        where = "WHERE status = ?"
        params.append(filter_status)
    params.append(limit)
    with read_tx() as conn:
        rows = conn.execute(
            "SELECT * FROM alert_notifications %s "
            "ORDER BY service, service_seq LIMIT ?" % where,
            params,
        ).fetchall()
        counts = {
            row["status"]: row["n"]
            for row in conn.execute(
                "SELECT status, COUNT(*) AS n FROM alert_notifications GROUP BY status"
            )
        }
        services = {
            row["service"]: row["n"]
            for row in conn.execute(
                "SELECT service, COUNT(*) AS n FROM alert_notifications "
                "WHERE status != 'delivered' GROUP BY service"
            )
        }
    return {
        "pending": counts.get("pending", 0),
        "sending": counts.get("sending", 0),
        "delivered": counts.get("delivered", 0),
        "undelivered_by_service": services,
        "notifications": [_notification_dict(r) for r in rows],
    }


# ---------------------------------------------------------------------------
# Claiming and completion (used by the delivery loop)
# ---------------------------------------------------------------------------

# A row may be claimed when it is pending and due, or when a previous sender
# claimed it but its lease expired (crash, hang).  A row is only eligible when
# no earlier, still-undelivered row exists for the same service: that is what
# keeps per-service delivery ordered even when several workers run.
_CLAIMABLE_SELECT = """
    (n.status = 'pending' AND n.next_attempt_at <= :now)
    OR (n.status = 'sending' AND n.lease_until IS NOT NULL AND n.lease_until <= :now)
"""

_CLAIMABLE_UPDATE = """
    (status = 'pending' AND next_attempt_at <= :now)
    OR (status = 'sending' AND lease_until IS NOT NULL AND lease_until <= :now)
"""


def claim_notification(
    conn: sqlite3.Connection, *, owner: str, now: float, lease_seconds: float
) -> sqlite3.Row | None:
    """Atomically lease the oldest deliverable notification.

    Caller must already hold a ``BEGIN IMMEDIATE`` write transaction; the
    update and the selection are therefore atomic with respect to other
    delivery workers.
    """
    row = conn.execute(
        "SELECT n.* FROM alert_notifications AS n "
        "WHERE (" + _CLAIMABLE_SELECT + ") "
        "AND NOT EXISTS ("
        "  SELECT 1 FROM alert_notifications AS o "
        "  WHERE o.service = n.service AND o.service_seq < n.service_seq "
        "  AND o.status != 'delivered'"
        ") "
        "ORDER BY n.rowid LIMIT 1",
        {"now": now},
    ).fetchone()
    if row is None:
        return None
    updated = conn.execute(
        "UPDATE alert_notifications "
        "SET status = 'sending', lease_owner = :owner, lease_until = :lease_until, "
        "    attempts = attempts + 1, last_error = NULL "
        "WHERE id = :id AND (" + _CLAIMABLE_UPDATE + ")",
        {
            "owner": owner,
            "lease_until": now + lease_seconds,
            "id": row["id"],
            "now": now,
        },
    )
    if updated.rowcount != 1:
        return None
    return conn.execute(
        "SELECT * FROM alert_notifications WHERE id = ?", (row["id"],)
    ).fetchone()


def mark_delivered(conn: sqlite3.Connection, notification_id: str) -> None:
    conn.execute(
        "UPDATE alert_notifications SET status = 'delivered', "
        "delivered_at = strftime('%Y-%m-%dT%H:%M:%fZ', 'now'), "
        "lease_owner = NULL, lease_until = NULL, last_error = NULL "
        "WHERE id = ?",
        (notification_id,),
    )


def mark_retry(
    conn: sqlite3.Connection, notification_id: str, *, error: str, delay: float
) -> None:
    conn.execute(
        "UPDATE alert_notifications SET status = 'pending', "
        "next_attempt_at = ?, lease_owner = NULL, lease_until = NULL, "
        "last_error = ? WHERE id = ?",
        (time.time() + delay, error[:1000], notification_id),
    )


def backoff_delay(attempts: int, base: float, cap: float) -> float:
    """Exponential backoff: base, 2*base, 4*base, ... capped at ``cap``."""
    delay = base * (2 ** max(0, attempts - 1))
    return min(delay, cap)
