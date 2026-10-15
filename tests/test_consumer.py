"""Unit tests for the Buenavista RabbitMQ consumer.

The tests mock message delivery metadata and RabbitMQ channel methods.
They verify validation, retries, dead-letter routing, and acknowledgements
without opening a broker connection or consuming real messages.
"""

import json
from unittest.mock import MagicMock

import pika
import pytest
from pydantic import ValidationError

from src.config import get_settings
from src.consumer import handle_message, process_event
from src.events import ReservationEvent, build_event


@pytest.fixture
def valid_event():
    """Provide a deterministic synthetic event for repeatable tests."""
    return build_event(index=0, seed=42)


@pytest.fixture
def delivery():
    """Create mocked RabbitMQ delivery metadata and channel methods."""
    channel = MagicMock()
    method = MagicMock()
    method.delivery_tag = 123

    properties = pika.BasicProperties(
        content_type="application/json",
        content_encoding="utf-8",
        headers={"schema_version": "1.0", "source": "unit-test"},
    )

    return channel, method, properties


def assert_confirmed_publish(channel, routing_key):
    """Verify that a persistent message was published to the expected queue."""
    channel.basic_publish.assert_called_once()

    call = channel.basic_publish.call_args
    assert call.kwargs["exchange"] == ""
    assert call.kwargs["routing_key"] == routing_key
    assert call.kwargs["mandatory"] is True

    properties = call.kwargs["properties"]
    assert properties.delivery_mode == 2


def test_valid_event_is_processed_and_acknowledged(valid_event, delivery):
    """A valid event must be processed before its delivery is acknowledged."""
    channel, method, properties = delivery
    body = valid_event.model_dump_json().encode("utf-8")
    processor = MagicMock()

    handle_message(
        channel=channel,
        method=method,
        properties=properties,
        body=body,
        processor=processor,
    )

    processor.assert_called_once()
    processed_event = processor.call_args.args[0]
    assert isinstance(processed_event, ReservationEvent)
    assert processed_event.event_id == valid_event.event_id

    channel.basic_ack.assert_called_once_with(delivery_tag=123)
    channel.basic_nack.assert_not_called()
    channel.basic_publish.assert_not_called()


def test_invalid_json_is_sent_to_dlq(delivery):
    """Malformed JSON must be dead-lettered rather than processed."""
    channel, method, properties = delivery
    processor = MagicMock()

    handle_message(
        channel=channel,
        method=method,
        properties=properties,
        body=b'{"event_id":',
        processor=processor,
    )

    processor.assert_not_called()
    assert_confirmed_publish(channel, get_settings().dead_letter_queue)

    published_properties = channel.basic_publish.call_args.kwargs["properties"]
    assert published_properties.headers["dead_letter_reason"] == "invalid_message"

    channel.basic_ack.assert_called_once_with(delivery_tag=123)
    channel.basic_nack.assert_not_called()


def test_invalid_schema_is_sent_to_dlq(valid_event, delivery):
    """Events that violate the shared schema must be dead-lettered."""
    channel, method, properties = delivery

    payload = valid_event.model_dump(mode="json")
    payload["unexpected_field"] = "not permitted"
    body = json.dumps(payload).encode("utf-8")
    processor = MagicMock()

    handle_message(
        channel=channel,
        method=method,
        properties=properties,
        body=body,
        processor=processor,
    )

    processor.assert_not_called()
    assert_confirmed_publish(channel, get_settings().dead_letter_queue)

    published_properties = channel.basic_publish.call_args.kwargs["properties"]
    assert published_properties.headers["dead_letter_reason"] == "invalid_message"

    channel.basic_ack.assert_called_once_with(delivery_tag=123)
    channel.basic_nack.assert_not_called()


def test_processing_failure_is_scheduled_for_retry(valid_event, delivery):
    """A temporary processing failure must be routed to the retry queue."""
    channel, method, properties = delivery
    body = valid_event.model_dump_json().encode("utf-8")
    processor = MagicMock(side_effect=RuntimeError("simulated processing failure"))

    handle_message(
        channel=channel,
        method=method,
        properties=properties,
        body=body,
        processor=processor,
    )

    processor.assert_called_once()
    assert_confirmed_publish(
        channel,
        f"{get_settings().queue_name}.retry",
    )

    published_properties = channel.basic_publish.call_args.kwargs["properties"]
    assert published_properties.headers["x-retry-count"] == 1

    channel.basic_ack.assert_called_once_with(delivery_tag=123)
    channel.basic_nack.assert_not_called()


def test_retry_exhaustion_routes_message_to_dlq(valid_event, delivery):
    """A message that has exhausted its retry allowance must reach the DLQ."""
    channel, method, properties = delivery
    settings = get_settings()

    properties.headers["x-retry-count"] = settings.max_retries
    body = valid_event.model_dump_json().encode("utf-8")
    processor = MagicMock(side_effect=RuntimeError("simulated processing failure"))

    handle_message(
        channel=channel,
        method=method,
        properties=properties,
        body=body,
        processor=processor,
    )

    processor.assert_called_once()
    assert_confirmed_publish(channel, settings.dead_letter_queue)

    published_properties = channel.basic_publish.call_args.kwargs["properties"]
    assert published_properties.headers["dead_letter_reason"] == "retry_limit_exceeded"

    channel.basic_ack.assert_called_once_with(delivery_tag=123)
    channel.basic_nack.assert_not_called()


def test_dlq_publish_failure_does_not_acknowledge(delivery):
    """A failed DLQ publication must not acknowledge the original message."""
    channel, method, properties = delivery
    channel.basic_publish.side_effect = pika.exceptions.AMQPError("simulated publication failure")

    with pytest.raises(pika.exceptions.AMQPError):
        handle_message(
            channel=channel,
            method=method,
            properties=properties,
            body=b"not-json",
            processor=MagicMock(),
        )

    channel.basic_ack.assert_not_called()


def test_retry_publish_failure_does_not_acknowledge(valid_event, delivery):
    """A failed retry publication must not acknowledge the original message."""
    channel, method, properties = delivery
    channel.basic_publish.side_effect = pika.exceptions.AMQPError("simulated publication failure")
    processor = MagicMock(side_effect=RuntimeError("temporary failure"))

    with pytest.raises(pika.exceptions.AMQPError):
        handle_message(
            channel=channel,
            method=method,
            properties=properties,
            body=valid_event.model_dump_json().encode("utf-8"),
            processor=processor,
        )

    processor.assert_called_once()
    channel.basic_ack.assert_not_called()


def test_default_processor_logs_valid_event(valid_event, caplog):
    """The default processor should log useful, non-sensitive event metadata."""
    with caplog.at_level("INFO", logger="src.consumer"):
        process_event(valid_event)

    assert str(valid_event.event_id) in caplog.text
    assert valid_event.event_type in caplog.text
    assert valid_event.hotel_id in caplog.text


def test_default_processor_accepts_valid_event(valid_event):
    """The development processor validates and logs but does not persist events."""
    process_event(valid_event)


def test_invalid_event_contract_rejects_extra_fields(valid_event):
    """The shared event schema must reject unreviewed fields."""
    payload = valid_event.model_dump(mode="json")
    payload["unexpected_field"] = "not permitted"

    with pytest.raises(ValidationError):
        ReservationEvent.model_validate(payload)
