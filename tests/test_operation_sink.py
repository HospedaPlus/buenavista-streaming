"""F2 events applied to F3's inventory (schema `operation`).

The unit tests check which F3 function each event calls, with a mocked
database. The integration tests run only when OPERATION_TEST_DATABASE_URL
points to a disposable PostgreSQL that has the F3 schema loaded
(db_simulated: `python -m seed all`) and migrations/001 applied; they are
skipped otherwise. Never point it at Neon production.
"""

import os
from datetime import UTC, datetime, timedelta
from unittest.mock import MagicMock

import psycopg
import pytest

from src.events import build_reservation_events
from src.neon_sink import NeonEventSink
from src.operation_sink import _apply, apply_event, f3_code


def lifecycle(number=1, seed=7, booked_at=None):
    """A reservation's events, booked now so its nights are on sale in F3."""
    booked_at = booked_at or datetime.now(UTC) - timedelta(minutes=5)
    return build_reservation_events(seed, number, booked_at)


def first_of(events, event_type):
    return next(e for e in events if e.event_type == event_type)


# ---------------------------------------------------------------- unit tests


@pytest.fixture
def cursor():
    cur = MagicMock()
    cur.fetchone.return_value = ("confirmed",)
    return cur


def test_created_calls_reservar_with_catalog_codes(cursor):
    event = lifecycle()[0]
    assert event.event_type == "reservation.created"

    assert _apply(cursor, event) == "confirmed"

    sql, params = cursor.execute.call_args.args
    assert "operation.reservar" in sql
    d = event.data
    assert params[:5] == (
        d.channel_code,
        event.reservation_id,
        event.hotel_id,
        d.room_type,
        d.guest_ref,
    )
    assert params[-2:] == (d.nightly_rate, event.occurred_at)


def test_update_for_unknown_booking_is_pending(cursor):
    events = [e for n in range(1, 40) for e in lifecycle(number=n)]
    update = first_of(events, "reservation.updated")
    cursor.fetchone.return_value = ("not_found",)

    assert _apply(cursor, update) == "pending"
    assert "operation.modificar" in cursor.execute.call_args.args[0]


def test_cancel_for_unknown_booking_is_pending(cursor):
    events = [e for n in range(1, 40) for e in lifecycle(number=n)]
    cancel = first_of(events, "reservation.cancelled")
    cursor.fetchone.return_value = None  # SELECT 1 ... finds nothing

    assert _apply(cursor, cancel) == "pending"


def test_check_in_and_out_do_not_touch_inventory(cursor):
    events = [e for n in range(1, 40) for e in lifecycle(number=n)]
    checked_in = first_of(events, "reservation.checked_in")

    assert _apply(cursor, checked_in) == "ignored"
    cursor.execute.assert_not_called()


def test_data_errors_are_skipped_not_raised():
    connection = MagicMock()
    connection.cursor.return_value.__enter__.return_value.execute.side_effect = (
        psycopg.errors.ForeignKeyViolation("unknown guest G-9999999")
    )
    assert apply_event(connection, lifecycle()[0]).startswith("skipped: unknown guest")


def test_redelivered_event_is_not_applied_twice(monkeypatch):
    connection = MagicMock()
    cursor = connection.cursor.return_value.__enter__.return_value
    event = lifecycle()[0]
    cursor.fetchone.side_effect = [None, (event.model_dump(mode="json"),)]  # already stored
    monkeypatch.setattr("src.neon_sink.psycopg.connect", MagicMock(return_value=connection))
    applied = MagicMock()
    monkeypatch.setattr("src.neon_sink.apply_event", applied)

    sink = NeonEventSink("postgresql://example.invalid/test", apply_operation=True)
    assert sink.persist(event, "t", 0, 1) is False
    applied.assert_not_called()


# --------------------------------------------------------- integration tests

DB_URL = os.getenv("OPERATION_TEST_DATABASE_URL")
needs_db = pytest.mark.skipif(not DB_URL, reason="OPERATION_TEST_DATABASE_URL not set")


def _row(code):
    with psycopg.connect(DB_URL) as conn:
        return conn.execute(
            "SELECT status, check_in, check_out, room_type_id FROM operation.reservations "
            "WHERE code = %s",
            (code,),
        ).fetchone()


def _sold(event):
    with psycopg.connect(DB_URL) as conn:
        return conn.execute(
            """SELECT coalesce(sum(i.sold_rooms), 0) FROM operation.inventory i
               JOIN operation.hotels h ON h.id = i.hotel_id
               JOIN operation.room_types t ON t.id = i.room_type_id
               WHERE h.code = %s AND t.code = %s
                 AND i.inventory_date >= %s AND i.inventory_date < %s""",
            (event.hotel_id, event.data.room_type, event.data.check_in, event.data.check_out),
        ).fetchone()[0]


@needs_db
def test_created_then_cancelled_moves_the_inventory():
    events = lifecycle(number=101, seed=int(datetime.now().timestamp()))
    created = events[0]
    before = _sold(created)
    sink = NeonEventSink(DB_URL, apply_operation=True)
    try:
        assert sink.persist(created, "it", 0, 1) is True
        assert sink.last_outcome in ("confirmed", "rejected")
        if sink.last_outcome == "rejected":
            pytest.skip("hotel full for this random stay")
        nights = (
            datetime.fromisoformat(created.data.check_out)
            - datetime.fromisoformat(created.data.check_in)
        ).days
        assert _sold(created) == before + nights
        assert _row(f3_code(created.reservation_id))[0] == "confirmed"

        # Same event again (Kafka redelivery): not applied twice.
        assert sink.persist(created, "it", 0, 1) is False
        assert _sold(created) == before + nights
    finally:
        sink.close()


@needs_db
def test_update_before_create_is_applied_when_the_creation_arrives():
    """CA-09: the update arrives first; it waits in the ledger and is applied after."""
    seed = int(datetime.now().timestamp()) + 1
    events = None
    for n in range(1, 200):
        candidate = lifecycle(number=n, seed=seed)
        if candidate[1].event_type == "reservation.updated":
            events = candidate
            break
    assert events, "no lifecycle with an update found"
    created, update = events[0], events[1]

    sink = NeonEventSink(DB_URL, apply_operation=True)
    try:
        sink.persist(update, "it", 0, 10)
        assert sink.last_outcome == "pending"
        sink.persist(created, "it", 0, 11)
        if sink.last_outcome.startswith("rejected"):
            pytest.skip("hotel full for this random stay")
        assert sink.last_outcome == "confirmed (+1 pending applied)"
        status, check_in, check_out, _ = _row(f3_code(created.reservation_id))
        assert status == "confirmed"
        assert (str(check_in), str(check_out)) == (update.data.check_in, update.data.check_out)
    finally:
        sink.close()
