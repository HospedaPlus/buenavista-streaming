"""Consume and validate reservation events from Apache Kafka.

The current processor logs validated event metadata only. Storage is handled
by the Neon ledger consumer (src/neon_consumer.py); the lakehouse reads Neon.
"""

import logging
import os
import signal

from confluent_kafka import Consumer, KafkaError, KafkaException
from pydantic import ValidationError

from src.events import ReservationEvent

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
logger = logging.getLogger(__name__)
shutdown_requested = False
POLL_TIMEOUT_SECONDS = 1.0


def request_shutdown(signum, frame) -> None:
    """Request a graceful stop after the current message."""
    del frame
    global shutdown_requested
    shutdown_requested = True
    logger.info("Shutdown requested: signal=%s", signum)


def process_event(event: ReservationEvent) -> None:
    """Log safe event metadata; storage sinks are not implemented in this stage."""
    logger.info(
        "Validated Kafka event event_id=%s type=%s hotel_id=%s reservation_id=%s version=%s",
        event.event_id,
        event.event_type,
        event.hotel_id,
        event.reservation_id,
        event.reservation_version,
    )


def consume_events() -> None:
    """Consume messages and commit offsets only after successful validation."""
    global shutdown_requested
    shutdown_requested = False
    broker = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
    topic = os.getenv("KAFKA_RESERVATION_TOPIC", "reservation.events.v1_3")
    group_id = os.getenv("KAFKA_GROUP_ID", "buenavista-validation-consumer")
    consumer = Consumer(
        {
            "bootstrap.servers": broker,
            "group.id": group_id,
            "auto.offset.reset": "earliest",
            "enable.auto.commit": False,
        }
    )
    signal.signal(signal.SIGINT, request_shutdown)
    signal.signal(signal.SIGTERM, request_shutdown)
    consumer.subscribe([topic])
    logger.info("Kafka consumer started broker=%s topic=%s group=%s", broker, topic, group_id)

    try:
        while not shutdown_requested:
            message = consumer.poll(POLL_TIMEOUT_SECONDS)
            if message is None:
                continue
            if message.error():
                if message.error().code() == KafkaError._PARTITION_EOF:
                    continue
                raise KafkaException(message.error())

            try:
                event = ReservationEvent.model_validate_json(message.value())
                process_event(event)
            except (ValidationError, ValueError):
                logger.exception(
                    "Invalid Kafka event; offset not committed topic=%s partition=%s offset=%s",
                    message.topic(),
                    message.partition(),
                    message.offset(),
                )
                # Stop rather than silently skipping invalid data. A dedicated
                # dead-letter topic will be added with the production sink stage.
                raise

            consumer.commit(message=message, asynchronous=False)
    finally:
        consumer.close()
        logger.info("Kafka consumer stopped")


if __name__ == "__main__":
    consume_events()
