"""Apply F2 reservation events to F3's operational inventory (schema `operation`).

The ledger keeps every event; this module makes OTA bookings take rooms from
the same inventory as direct bookings, through the functions in the F3 repo
(db_simulated, sql/functions.sql). It runs inside the ledger transaction, so
the ledger row and the inventory change commit together or not at all.

    reservation.created    -> operation.reservar()
    reservation.updated    -> operation.modificar()
    reservation.cancelled  -> operation.cancelar()
    reservation.no_show    -> operation.liberar(..., 'no_show')
    checked_in / out       -> nothing (no inventory change)

Out-of-order events (CA-09): an update, cancellation or no-show for a booking
F3 doesn't know yet stays in the ledger as "pending". When its creation
arrives, the later events of that reservation are replayed from the ledger in
version order.
"""

from __future__ import annotations

import logging
from typing import Any

import psycopg

from src.events import ReservationEvent

logger = logging.getLogger(__name__)

# F3 prefixes the locator of every F2 booking with its source.
F3_CODE_PREFIX = "F2-"

# Data problems F3 rejects (unknown hotel/guest, a stay that starts before the
# booking day, a night with no rate). The event stays in the ledger, the
# inventory is untouched and the consumer keeps going.
SKIPPABLE_ERRORS = (
    psycopg.errors.CheckViolation,
    psycopg.errors.ForeignKeyViolation,
    psycopg.errors.RaiseException,
)


def f3_code(reservation_id: str) -> str:
    """Locator F3 gives an F2 booking (reservations.code)."""
    return F3_CODE_PREFIX + reservation_id


def apply_event(connection: psycopg.Connection, event: ReservationEvent) -> str:
    """Apply one new event to `operation` and return what happened.

    Must run inside the caller's transaction. Data errors are contained in a
    savepoint: the result is 'skipped: ...' and the ledger insert still commits.
    """
    try:
        with connection.transaction():  # savepoint
            with connection.cursor() as cursor:
                outcome = _apply(cursor, event)
                if event.event_type == "reservation.created" and outcome in (
                    "confirmed",
                    "already_exists",
                ):
                    replayed = _replay_pending(cursor, event)
                    if replayed:
                        outcome += f" (+{replayed} pending applied)"
                return outcome
    except SKIPPABLE_ERRORS as error:
        message = str(error).strip().splitlines()[0][:200]
        logger.warning(
            "Event not applied to operation event_id=%s reservation_id=%s: %s",
            event.event_id,
            event.reservation_id,
            message,
        )
        return f"skipped: {message}"


def _apply(cursor: psycopg.Cursor, event: ReservationEvent) -> str:
    d = event.data
    if event.event_type == "reservation.created":
        cursor.execute(
            # Explicit casts: psycopg sends small ints as smallint and dates as
            # text, and PostgreSQL only matches the function with exact types.
            "SELECT result FROM operation.reservar(%s::text, %s::text, %s::text, %s::text,"
            " %s::text, %s::date, %s::date, 1, %s::int, %s::boolean, 0, 0, %s::numeric,"
            " %s::timestamptz)",
            (
                d.channel_code,
                event.reservation_id,
                event.hotel_id,
                d.room_type,
                d.guest_ref,
                d.check_in,
                d.check_out,
                d.guests,
                d.refundable,
                d.nightly_rate,
                event.occurred_at,
            ),
        )
        return cursor.fetchone()[0]

    if event.event_type == "reservation.updated":
        cursor.execute(
            "SELECT result FROM operation.modificar(%s::text, %s::text, %s::text, %s::date,"
            " %s::date, 1, %s::int, 0, 0, %s::numeric, %s::timestamptz)",
            (
                d.channel_code,
                event.reservation_id,
                d.room_type,
                d.check_in,
                d.check_out,
                d.guests,
                d.nightly_rate,
                event.occurred_at,
            ),
        )
        result = cursor.fetchone()[0]
        return "pending" if result == "not_found" else result

    if event.event_type in ("reservation.cancelled", "reservation.no_show"):
        if not _exists(cursor, event):
            return "pending"
        if event.event_type == "reservation.cancelled":
            cursor.execute(
                "SELECT result FROM operation.cancelar(%s::text, %s::timestamptz)",
                (f3_code(event.reservation_id), event.occurred_at),
            )
            return cursor.fetchone()[0]
        cursor.execute(
            "SELECT operation.liberar(%s::text, 'no_show')", (f3_code(event.reservation_id),)
        )
        return "no_show" if cursor.fetchone()[0] else "not_modifiable"

    return "ignored"  # checked_in / checked_out don't change the inventory


def _exists(cursor: psycopg.Cursor, event: ReservationEvent) -> bool:
    cursor.execute(
        "SELECT 1 FROM operation.reservations WHERE code = %s",
        (f3_code(event.reservation_id),),
    )
    return cursor.fetchone() is not None


def _replay_pending(cursor: psycopg.Cursor, created: ReservationEvent) -> int:
    """Apply, in version order, the events of this reservation that arrived
    before its creation (they are already in the ledger)."""
    cursor.execute(
        """
        SELECT payload
        FROM streaming.reservation_event_ledger
        WHERE reservation_id = %s AND reservation_version > %s
        ORDER BY reservation_version
        """,
        (created.reservation_id, created.reservation_version),
    )
    later: list[Any] = [row[0] for row in cursor.fetchall()]
    for payload in later:
        event = ReservationEvent.model_validate(payload)
        outcome = _apply(cursor, event)
        logger.info(
            "Pending event applied event_id=%s type=%s outcome=%s",
            event.event_id,
            event.event_type,
            outcome,
        )
    return len(later)
