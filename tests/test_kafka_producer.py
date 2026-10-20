"""Focused tests for Kafka delivery callback failure propagation."""

import sys
from unittest.mock import MagicMock

import pytest

from src import kafka_producer
from src.events import build_event


@pytest.fixture
def producer_with_delivery_result(monkeypatch):
    """Provide a producer that runs its callback when flush drains the queue."""
    producer = MagicMock()
    callback = None
    delivery_error = None

    def produce(*args, **kwargs):
        nonlocal callback
        callback = kwargs["on_delivery"]

    def flush(timeout):
        assert timeout == 30
        callback(delivery_error, MagicMock())
        return 0

    producer.produce.side_effect = produce
    producer.flush.side_effect = flush
    monkeypatch.setattr(kafka_producer, "Producer", MagicMock(return_value=producer))
    monkeypatch.setattr(kafka_producer, "generate_events", lambda **_: [build_event(index=0)])

    def set_delivery_error(error):
        nonlocal delivery_error
        delivery_error = error

    return producer, set_delivery_error


def test_publish_succeeds_when_delivery_callback_confirms_message(producer_with_delivery_result):
    producer, _ = producer_with_delivery_result

    assert kafka_producer.publish_events(count=1) == 1

    producer.flush.assert_called_once_with(30)


def test_delivery_callback_failure_makes_cli_fail(monkeypatch, producer_with_delivery_result):
    producer, set_delivery_error = producer_with_delivery_result
    set_delivery_error(RuntimeError("broker rejected message"))
    monkeypatch.setattr(sys, "argv", ["kafka_producer", "--count", "1"])

    with pytest.raises(kafka_producer.KafkaDeliveryError, match="broker rejected message"):
        kafka_producer.main()

    producer.flush.assert_called_once_with(30)
