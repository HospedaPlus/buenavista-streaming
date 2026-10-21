"""Tests for Neon consumer offset ordering and safe shutdown."""

from unittest.mock import MagicMock

import pytest

from src import neon_consumer
from src.events import build_event


@pytest.fixture
def stub_signal_handlers(monkeypatch):
    """Prevent the consumer from changing the test runner's signal handlers."""
    monkeypatch.setattr(neon_consumer.signal, "signal", MagicMock())
    neon_consumer.shutdown_requested = False
    yield
    neon_consumer.shutdown_requested = False


def test_offset_is_committed_only_after_neon_persistence(
    monkeypatch, stub_signal_handlers
):
    """A successful database write must precede the Kafka offset commit."""
    event = build_event(index=0, seed=42)
    message = MagicMock()
    message.error.return_value = None
    message.value.return_value = event.model_dump_json().encode("utf-8")
    consumer = MagicMock()
    sink = MagicMock()
    actions = []

    def persist(**kwargs):
        actions.append("persist")
        assert kwargs["event"].event_id == event.event_id
        neon_consumer.shutdown_requested = True
        return True

    sink.persist.side_effect = persist
    consumer.poll.return_value = message
    consumer.commit.side_effect = lambda **_: actions.append("commit")
    consumer.close.side_effect = lambda: actions.append("close")

    monkeypatch.setattr(neon_consumer, "Consumer", MagicMock(return_value=consumer))
    monkeypatch.setattr(neon_consumer, "NeonEventSink", MagicMock(return_value=sink))

    neon_consumer.consume_events()

    assert actions == ["persist", "commit", "close"]
    consumer.commit.assert_called_once_with(message=message, asynchronous=False)


def test_failed_neon_persistence_does_not_commit_offset(
    monkeypatch, stub_signal_handlers
):
    """Database failures leave the message offset available for replay."""
    event = build_event(index=0, seed=42)
    message = MagicMock()
    message.error.return_value = None
    message.value.return_value = event.model_dump_json().encode("utf-8")
    consumer = MagicMock()
    sink = MagicMock()
    sink.persist.side_effect = RuntimeError("database unavailable")
    consumer.poll.return_value = message

    monkeypatch.setattr(neon_consumer, "Consumer", MagicMock(return_value=consumer))
    monkeypatch.setattr(neon_consumer, "NeonEventSink", MagicMock(return_value=sink))

    with pytest.raises(RuntimeError, match="database unavailable"):
        neon_consumer.consume_events()

    consumer.commit.assert_not_called()
    consumer.close.assert_called_once_with()
