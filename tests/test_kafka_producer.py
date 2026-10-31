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


def test_rate_profile_has_a_peak_window_every_cycle():
    """The default profile sends 15 ev/s during the first 10 s of each minute, else 5."""
    profile = kafka_producer.RateProfile()
    assert profile.rate_at(0) == 15.0
    assert profile.rate_at(9.9) == 15.0
    assert profile.rate_at(10) == 5.0
    assert profile.rate_at(61) == 15.0
    with pytest.raises(ValueError):
        kafka_producer.RateProfile(base_rate=0)


class FakeClock:
    """A clock that only moves when the producer sleeps, so tests run instantly."""

    def __init__(self):
        self.now = 0.0

    def __call__(self):
        return self.now

    def sleep(self, seconds):
        self.now += seconds


def test_continuous_mode_reaches_at_least_10_events_per_second_at_peak(monkeypatch):
    """F2 requires >= 10 events/s at peak; the meter records the best full second."""
    producer = MagicMock()
    producer.flush.return_value = 0
    monkeypatch.setattr(kafka_producer, "Producer", MagicMock(return_value=producer))
    clock = FakeClock()
    meters = []
    original_meter = kafka_producer.ThroughputMeter

    def tracking_meter():
        meter = original_meter()
        meters.append(meter)
        return meter

    monkeypatch.setattr(kafka_producer, "ThroughputMeter", tracking_meter)

    sent = kafka_producer.publish_events(
        count=None,
        rate_profile=kafka_producer.RateProfile(),
        duration_s=30,
        clock=clock,
        sleep=clock.sleep,
    )

    # 10 s at 15 ev/s + 20 s at 5 ev/s = about 250 events.
    assert 230 <= sent <= 270
    assert meters[0].peak_per_second >= 10
    keys = {call.kwargs["key"] for call in producer.produce.call_args_list}
    assert all(key.startswith(b"F2-RES-") for key in keys)


def test_producer_waits_for_all_replicas_and_is_idempotent(monkeypatch):
    """acks=all and idempotence protect against losing or duplicating events (RNF-05)."""
    producer_class = MagicMock()
    producer = producer_class.return_value
    producer.flush.return_value = 0
    monkeypatch.setattr(kafka_producer, "Producer", producer_class)

    kafka_producer.publish_events(count=3)

    config = producer_class.call_args.args[0]
    assert config["acks"] == "all"
    assert config["enable.idempotence"] is True
