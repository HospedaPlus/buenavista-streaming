"""Unit tests for Neon event-ledger idempotency and payload-conflict handling."""

from unittest.mock import MagicMock

import pytest

from src.events import build_event
from src.neon_sink import NeonEventSink


@pytest.fixture
def database_mocks(monkeypatch):
    """Provide an isolated psycopg connection and cursor without a live database."""
    connection = MagicMock()
    cursor = MagicMock()
    connection.__enter__.return_value = connection
    connection.cursor.return_value.__enter__.return_value = cursor
    monkeypatch.setattr("src.neon_sink.psycopg.connect", MagicMock(return_value=connection))
    return connection, cursor


def test_sink_requires_a_database_url(monkeypatch):
    """Fail early rather than starting a consumer with no configured destination."""
    monkeypatch.delenv("NEON_DATABASE_URL", raising=False)

    with pytest.raises(ValueError, match="NEON_DATABASE_URL"):
        NeonEventSink()


def test_persist_returns_true_for_a_new_event(database_mocks):
    """A first insert is reported as newly persisted."""
    _, cursor = database_mocks
    event = build_event(index=0, seed=42)
    cursor.fetchone.return_value = (str(event.event_id),)
    sink = NeonEventSink("postgresql://example.invalid/test")

    inserted = sink.persist(event, "reservation.events.v1_2", 1, 25)

    assert inserted is True
    assert cursor.execute.call_count == 1
    sql, params = cursor.execute.call_args.args
    assert "INSERT INTO public.reservation_event_ledger" in sql
    assert params[0] == str(event.event_id)
    assert params[-3:] == ("reservation.events.v1_2", 1, 25)


def test_persist_accepts_a_duplicate_with_the_same_payload(database_mocks):
    """An already stored event is idempotent when its payload matches."""
    _, cursor = database_mocks
    event = build_event(index=0, seed=42)
    payload = event.model_dump(mode="json")
    cursor.fetchone.side_effect = [None, (payload,)]
    sink = NeonEventSink("postgresql://example.invalid/test")

    inserted = sink.persist(event, "reservation.events.v1_2", 0, 7)

    assert inserted is False
    assert cursor.execute.call_count == 2


def test_persist_rejects_event_id_reuse_with_a_different_payload(database_mocks):
    """A conflicting payload must not be silently treated as a harmless duplicate."""
    _, cursor = database_mocks
    event = build_event(index=0, seed=42)
    cursor.fetchone.side_effect = [None, ({"event_id": "different-payload"},)]
    sink = NeonEventSink("postgresql://example.invalid/test")

    with pytest.raises(ValueError, match="different payload"):
        sink.persist(event, "reservation.events.v1_2", 0, 8)
