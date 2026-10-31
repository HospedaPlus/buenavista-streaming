"""Tests for Neon consumer offset ordering and safe shutdown."""

from unittest.mock import MagicMock

import pytest

from src import neon_consumer
from src.events import build_event


@pytest.fixture
def stub_signal_handlers(monkeypatch):
    """Prevent the consumer from changing the test runner's signal handlers."""
    monkeypatch.setattr(neon_consumer.signal, "signal", MagicMock())
    # No real Kafka client for the dead-letter producer in unit tests.
    monkeypatch.setattr(neon_consumer, "Producer", MagicMock())
    neon_consumer.shutdown_requested = False
    yield
    neon_consumer.shutdown_requested = False


def test_offset_is_committed_only_after_neon_persistence(monkeypatch, stub_signal_handlers):
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


def test_failed_neon_persistence_does_not_commit_offset(monkeypatch, stub_signal_handlers):
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


def test_invalid_message_goes_to_dlq_and_stream_continues(monkeypatch, stub_signal_handlers):
    """A bad message is copied to the DLQ, its offset committed, and nothing is persisted."""
    message = MagicMock()
    message.error.return_value = None
    message.value.return_value = b'{"not": "a valid event"}'
    message.key.return_value = b"F2-RES-0042-00000001"
    message.headers.return_value = [("event_type", b"reservation.created")]
    message.topic.return_value = "reservation.events.v1_3"
    message.partition.return_value = 2
    message.offset.return_value = 99
    consumer = MagicMock()
    sink = MagicMock()
    dlq = MagicMock()
    dlq.flush.return_value = 0
    actions = []

    def poll(_timeout):
        if actions:
            neon_consumer.shutdown_requested = True
            return None
        return message

    consumer.poll.side_effect = poll
    dlq.produce.side_effect = lambda *a, **k: actions.append("dlq")
    consumer.commit.side_effect = lambda **_: actions.append("commit")

    monkeypatch.setattr(neon_consumer, "Consumer", MagicMock(return_value=consumer))
    monkeypatch.setattr(neon_consumer, "Producer", MagicMock(return_value=dlq))  # overrides fixture
    monkeypatch.setattr(neon_consumer, "NeonEventSink", MagicMock(return_value=sink))

    neon_consumer.consume_events()

    assert actions == ["dlq", "commit"]
    sink.persist.assert_not_called()
    topic = dlq.produce.call_args.args[0]
    headers = dict(dlq.produce.call_args.kwargs["headers"])
    assert topic == "reservation.events.dlq"
    assert headers["dlq_source_offset"] == b"99"
    assert b"validation error" in headers["dlq_error"].lower()
