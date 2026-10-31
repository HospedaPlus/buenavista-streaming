"""Focused tests for Kafka polling, commit ordering, and graceful shutdown."""

from unittest.mock import MagicMock

import pytest

from src import kafka_consumer
from src.events import build_event


@pytest.fixture
def stub_signal_handlers(monkeypatch):
    """Keep consumer tests from changing the test runner's process handlers."""
    kafka_consumer.shutdown_requested = False
    monkeypatch.setattr(kafka_consumer.signal, "signal", MagicMock())
    yield
    kafka_consumer.shutdown_requested = False


def test_consumer_polls_with_bounded_wait_and_commits_after_processing(
    monkeypatch, stub_signal_handlers
):
    """One record is processed and synced before graceful shutdown closes the client."""
    event = build_event(index=0, seed=42)
    message = MagicMock()
    message.error.return_value = None
    message.value.return_value = event.model_dump_json().encode("utf-8")
    consumer = MagicMock()
    actions = []

    def process_event(received_event):
        actions.append("process")
        assert received_event.event_id == event.event_id
        kafka_consumer.shutdown_requested = True

    consumer.poll.return_value = message
    consumer.commit.side_effect = lambda **_: actions.append("commit")
    consumer.close.side_effect = lambda: actions.append("close")
    monkeypatch.setattr(kafka_consumer, "Consumer", MagicMock(return_value=consumer))
    monkeypatch.setattr(kafka_consumer, "process_event", process_event)

    kafka_consumer.consume_events()

    consumer.poll.assert_called_once_with(kafka_consumer.POLL_TIMEOUT_SECONDS)
    consumer.subscribe.assert_called_once_with(["reservation.events.v1_3"])
    consumer.commit.assert_called_once_with(message=message, asynchronous=False)
    assert actions == ["process", "commit", "close"]


def test_invalid_message_is_not_committed_and_consumer_still_closes(
    monkeypatch, stub_signal_handlers
):
    """Validation failures leave the offset uncommitted and close the group member."""
    message = MagicMock()
    message.error.return_value = None
    message.value.return_value = b"not-json"
    consumer = MagicMock()
    consumer.poll.return_value = message
    monkeypatch.setattr(kafka_consumer, "Consumer", MagicMock(return_value=consumer))

    with pytest.raises(ValueError):
        kafka_consumer.consume_events()

    consumer.commit.assert_not_called()
    consumer.close.assert_called_once_with()
