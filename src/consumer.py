"""Consume, validate, and route F2 reservation events through RabbitMQ.

This module implements the consumer stage of the Buenavista streaming
ingestion pipeline.

Responsibilities:
    - Connect to RabbitMQ using the shared application configuration.
    - Consume reservation events using manual acknowledgements.
    - Validate messages against the shared Pydantic event contract.
    - Retry transient processing failures with a configurable delay.
    - Route invalid and repeatedly failing messages to a dead-letter queue.
    - Support graceful application shutdown.

The current process_event() implementation validates and logs events only.
It does not persist them to a database or object-storage destination.

Reliability note:
    Message publication and acknowledgement are not one atomic transaction.
    This implementation uses publisher confirms before acknowledging or
    transferring a delivery, reducing the risk of losing a message during
    ordinary routing failures. Duplicate delivery remains possible, so
    downstream processing should be idempotent.
"""

import json
import logging
import signal
import sys
from collections.abc import Callable
from typing import Any

import pika
from pika.exceptions import AMQPError
from pydantic import ValidationError

from src.config import get_settings
from src.events import ReservationEvent
from src.producer import create_connection, declare_stream_topology

# Keep application logs consistent for troubleshooting and automated tests.
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(name)s %(message)s",
)
logger = logging.getLogger(__name__)

# Injecting the processor allows message handling to be tested independently
# from RabbitMQ and lets downstream storage be added without changing routing.
EventProcessor = Callable[[ReservationEvent], None]

# The signal handler sets this flag so the main loop can stop gracefully.
shutdown_requested = False


def request_shutdown(signum: int, frame: Any) -> None:
    """Request controlled shutdown without closing the connection in a signal handler."""
    del frame

    # The consumer loop checks this flag and stops requesting new deliveries.
    global shutdown_requested
    shutdown_requested = True

    logger.info("Shutdown requested; signal=%s", signum)


def process_event(event: ReservationEvent) -> None:
    """Process a validated event.

    This development implementation logs non-sensitive event metadata.
    Replace or extend it when the team confirms the downstream destination.

    A production implementation must return successfully only after the
    required processing operation has completed.
    """
    # Avoid logging the entire payload because future schemas may contain
    # information that should not appear in application logs.
    logger.info(
        "Validated event: event_id=%s event_type=%s hotel_id=%s reservation_id=%s version=%s",
        event.event_id,
        event.event_type,
        event.hotel_id,
        event.reservation_id,
        event.reservation_version,
    )


def declare_topology(channel: Any) -> None:
    """Declare the retry and dead-letter queues without recreating the main queue.

    The retry queue uses a message TTL and dead-letters expired messages back
    to the existing main queue through RabbitMQ's default exchange.

    The dead-letter queue retains invalid messages and messages that exhaust
    the configured retry attempts.

    The main queue is intentionally not redeclared with new queue arguments.
    Changing its arguments can cause a PRECONDITION_FAILED error, and deleting
    it to recreate it would risk losing messages already waiting in it.
    """
    settings = get_settings()

    # Retain invalid and repeatedly failing messages for later investigation.
    channel.queue_declare(
        queue=settings.dead_letter_queue,
        durable=True,
    )

    # Expired retry messages return to the main queue. The queue-level TTL
    # provides a delay without blocking the consumer with time.sleep().
    retry_queue = f"{settings.queue_name}.retry"
    channel.queue_declare(
        queue=retry_queue,
        durable=True,
        arguments={
            "x-message-ttl": settings.retry_delay_ms,
            "x-dead-letter-exchange": "",
            "x-dead-letter-routing-key": settings.queue_name,
        },
    )

    logger.info(
        "RabbitMQ topology declared: main_queue=%s retry_queue=%s "
        "dead_letter_queue=%s retry_delay_ms=%s max_retries=%s",
        settings.queue_name,
        retry_queue,
        settings.dead_letter_queue,
        settings.retry_delay_ms,
        settings.max_retries,
    )


def publish_confirmed(
    channel: Any,
    queue_name: str,
    body: bytes,
    properties: pika.BasicProperties,
) -> None:
    """Publish a persistent message and require broker confirmation.

    The original delivery must not be acknowledged until RabbitMQ confirms
    that the replacement message has been accepted. Otherwise, a failed
    transfer could lose the event.
    """
    # The channel has publisher confirms enabled (see consume_events), so a
    # successful return from basic_publish() represents broker confirmation and
    # a rejected message raises NackError or UnroutableError.

    # Use the default exchange and the destination queue name as the routing key.
    # Persistent delivery helps messages survive broker restarts when the
    # broker's durability and storage configuration support it.
    confirmed = channel.basic_publish(
        exchange="",
        routing_key=queue_name,
        body=body,
        properties=properties,
        mandatory=True,
    )

    if confirmed is False:
        raise RuntimeError(f"RabbitMQ did not confirm publication to queue {queue_name}")


def route_to_dead_letter(
    channel: Any,
    method: Any,
    body: bytes,
    properties: pika.BasicProperties,
    reason: str,
) -> None:
    """Transfer an invalid or exhausted message to the dead-letter queue.

    The original delivery is acknowledged only after the dead-letter
    publication succeeds. This avoids intentionally discarding the source
    delivery before its replacement has been confirmed.
    """
    settings = get_settings()

    # Copy the existing headers so the original event metadata is preserved.
    headers = dict(properties.headers or {})
    headers["dead_letter_reason"] = reason

    # Build new properties instead of mutating the incoming delivery properties.
    dead_letter_properties = pika.BasicProperties(
        content_type=properties.content_type or "application/json",
        content_encoding=properties.content_encoding or "utf-8",
        delivery_mode=2,
        message_id=properties.message_id,
        correlation_id=properties.correlation_id,
        type=properties.type,
        timestamp=properties.timestamp,
        headers=headers,
    )

    # Confirm the dead-letter copy before acknowledging the original message.
    publish_confirmed(
        channel=channel,
        queue_name=settings.dead_letter_queue,
        body=body,
        properties=dead_letter_properties,
    )

    channel.basic_ack(delivery_tag=method.delivery_tag)

    logger.warning(
        "Message transferred to dead-letter queue: destination=%s "
        "reason=%s delivery_tag=%s message_id=%s",
        settings.dead_letter_queue,
        reason,
        method.delivery_tag,
        properties.message_id,
    )


def handle_message(
    channel: Any,
    method: Any,
    properties: Any,
    body: bytes,
    processor: EventProcessor | None = None,
) -> None:
    """Validate, process, retry, or dead-letter one RabbitMQ delivery.

    Invalid JSON and schema violations go directly to the dead-letter queue.
    Transient processing failures are sent to the retry queue until the
    configured retry limit is reached.

    A replacement message is published and confirmed before the original
    delivery is acknowledged. This reduces message-loss risk but can produce
    duplicates if the connection fails between publication and acknowledgement.

    Args:
        channel: RabbitMQ channel used for publishing and acknowledgements.
        method: Delivery metadata supplied by RabbitMQ.
        properties: Original AMQP message properties.
        body: Raw message body.
        processor: Optional processing function used for dependency injection.
    """
    settings = get_settings()
    event_processor = processor or process_event

    # The producer normally sends BasicProperties, but defensive defaults make
    # the handler easier to test and safer with messages from other publishers.
    if properties is None:
        properties = pika.BasicProperties()

    try:
        # Decode and parse the incoming message before validating its schema.
        payload = json.loads(body.decode("utf-8"))

        # Reuse the producer's event contract to prevent schema drift.
        event = ReservationEvent.model_validate(payload)

    except (UnicodeDecodeError, json.JSONDecodeError, ValidationError):
        # Invalid messages will not become valid through ordinary retries.
        # Retain them for investigation rather than creating a redelivery loop.
        logger.exception(
            "Message validation failed: delivery_tag=%s",
            method.delivery_tag,
        )

        try:
            route_to_dead_letter(
                channel=channel,
                method=method,
                body=body,
                properties=properties,
                reason="invalid_message",
            )
        except AMQPError:
            # If routing fails, do not acknowledge the original delivery.
            # Let the connection's recovery/redelivery behavior handle it.
            logger.exception("Could not transfer invalid message to dead-letter queue")
            raise
        return

    try:
        # Invoke the processing layer only after schema validation succeeds.
        event_processor(event)

    except Exception:
        # Read the retry counter from message headers. Each confirmed transfer
        # to the retry queue increments this value.
        headers = dict(properties.headers or {})
        retry_count = int(headers.get("x-retry-count", 0))

        logger.exception(
            "Processing failed: event_id=%s retry_count=%s max_retries=%s",
            event.event_id,
            retry_count,
            settings.max_retries,
        )

        if retry_count < settings.max_retries:
            # Preserve the original headers and increment the retry counter.
            headers["x-retry-count"] = retry_count + 1

            retry_properties = pika.BasicProperties(
                content_type=properties.content_type or "application/json",
                content_encoding=properties.content_encoding or "utf-8",
                delivery_mode=2,
                message_id=properties.message_id or str(event.event_id),
                correlation_id=properties.correlation_id,
                type=properties.type or event.event_type,
                timestamp=properties.timestamp,
                headers=headers,
            )

            retry_queue = f"{settings.queue_name}.retry"

            # Publish to the TTL-based retry queue and wait for confirmation.
            # RabbitMQ will return the message to the main queue after expiry.
            publish_confirmed(
                channel=channel,
                queue_name=retry_queue,
                body=body,
                properties=retry_properties,
            )

            # Acknowledge the original only after the retry copy is confirmed.
            channel.basic_ack(delivery_tag=method.delivery_tag)

            logger.warning(
                "Message scheduled for retry: event_id=%s retry_attempt=%s retry_queue=%s",
                event.event_id,
                retry_count + 1,
                retry_queue,
            )
            return

        # The retry limit has been exhausted. Transfer the event to the
        # dead-letter queue instead of requeueing it indefinitely.
        try:
            route_to_dead_letter(
                channel=channel,
                method=method,
                body=body,
                properties=properties,
                reason="retry_limit_exceeded",
            )
        except AMQPError:
            logger.exception("Could not transfer exhausted message to dead-letter queue")
            raise
        return

    # Acknowledge only after processing completes successfully.
    # The current processor logs metadata; it does not persist the event.
    channel.basic_ack(delivery_tag=method.delivery_tag)

    logger.debug(
        "Message acknowledged: event_id=%s delivery_tag=%s",
        event.event_id,
        method.delivery_tag,
    )


def consume_events() -> None:
    """Declare the queues and consume messages until shutdown."""
    settings = get_settings()
    connection = create_connection()

    try:
        channel = connection.channel()

        # Declare the exchange, the main queue with its original settings, and
        # their binding. Nothing is deleted, so waiting messages are kept.
        declare_stream_topology(channel)

        # Enable publisher confirms once per channel; retry and dead-letter
        # transfers rely on them before acknowledging the original delivery.
        channel.confirm_delivery()

        # Declare supporting queues before registering the consumer.
        declare_topology(channel)

        # Limit in-flight deliveries to provide basic backpressure.
        channel.basic_qos(prefetch_count=1)

        # Manual acknowledgements ensure the handler controls each delivery's
        # success, retry, or dead-letter outcome.
        channel.basic_consume(
            queue=settings.queue_name,
            on_message_callback=handle_message,
            auto_ack=False,
        )

        logger.info(
            "Consumer started; queue=%s. Waiting for reservation events.",
            settings.queue_name,
        )

        # Process broker events in short intervals so shutdown requests
        # can be handled without waiting indefinitely.
        while not shutdown_requested and connection.is_open:
            connection.process_data_events(time_limit=1)

        if channel.is_open:
            channel.stop_consuming()

    finally:
        # Release the connection even if the consumer exits unexpectedly.
        if connection.is_open:
            connection.close()

        logger.info("Consumer stopped.")


def main() -> None:
    """Register signal handlers and run the consumer."""
    signal.signal(signal.SIGINT, request_shutdown)
    signal.signal(signal.SIGTERM, request_shutdown)

    try:
        consume_events()
    except AMQPError:
        logger.exception("RabbitMQ consumer failed.")
        sys.exit(1)


# Keep imports safe for unit tests: connecting to RabbitMQ happens only when
# the application is explicitly executed.
if __name__ == "__main__":
    main()
