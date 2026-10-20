"""Consume Kafka reservation events and persist them to the Neon event ledger.

Processing order:
1. Receive a Kafka message.
2. Validate the event contract.
3. Persist the event and Kafka provenance to Neon.
4. Commit the Kafka offset only after the database transaction succeeds.

The operational reservations table is not modified by this consumer.
"""

import logging
import os
import signal
from typing import Any

from confluent_kafka import Consumer, KafkaError, KafkaException
from pydantic import ValidationError

from src.events import ReservationEvent
from src.neon_sink import NeonEventSink

# Configure logging without exposing credentials or full event payloads.
logging.basicConfig(
    level=os.getenv("LOG_LEVEL", "INFO").upper(),
    format="%(asctime)s %(levelname)s %(name)s %(message)s",
)
logger = logging.getLogger(__name__)

# Signal handlers set this flag to request a graceful shutdown.
shutdown_requested = False

# Maximum time to wait for a Kafka message.
POLL_TIMEOUT_SECONDS = 1.0


def request_shutdown(signum: int, frame: Any) -> None:
    """Request shutdown after the current processing operation."""
    del frame
    global shutdown_requested
    shutdown_requested = True
    logger.info("Shutdown requested: signal=%s", signum)


def process_event(
    event: ReservationEvent,
    message: Any,
    sink: NeonEventSink,
) -> bool:
    """Persist one event and return whether a new ledger row was inserted."""
    inserted = sink.persist(
        event=event,
        topic=message.topic(),
        partition=message.partition(),
        offset=message.offset(),
    )

    # Log provenance and event ID, not credentials or the full payload.
    logger.info(
        "Neon persistence completed event_id=%s inserted=%s "
        "topic=%s partition=%s offset=%s",
        event.event_id,
        inserted,
        message.topic(),
        message.partition(),
        message.offset(),
    )
    return inserted


def consume_events() -> None:
    """Consume events and commit offsets only after successful Neon writes."""
    global shutdown_requested
    shutdown_requested = False

    # Use environment configuration with local development defaults.
    broker = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
    topic = os.getenv("KAFKA_RESERVATION_TOPIC", "reservation.events.v1_2")
    group_id = os.getenv("KAFKA_GROUP_ID", "buenavista-neon-sink")

    # Disable automatic commits so database writes happen first.
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

    logger.info(
        "Kafka consumer starting broker=%s topic=%s group=%s",
        broker,
        topic,
        group_id,
    )

    try:
        # The sink reads NEON_DATABASE_URL from the environment.
        sink = NeonEventSink()

        while not shutdown_requested:
            message = consumer.poll(POLL_TIMEOUT_SECONDS)

            if message is None:
                continue

            if message.error():
                if message.error().code() == KafkaError._PARTITION_EOF:
                    continue
                raise KafkaException(message.error())

            try:
                # Reject messages that do not match the event contract.
                event = ReservationEvent.model_validate_json(message.value())

                # If Neon fails, execution raises and the offset is not committed.
                process_event(event, message, sink)

            except (ValidationError, ValueError):
                logger.exception(
                    "Invalid or inconsistent event; offset not committed "
                    "topic=%s partition=%s offset=%s",
                    message.topic(),
                    message.partition(),
                    message.offset(),
                )
                raise

            except Exception:
                logger.exception(
                    "Event persistence failed; offset not committed "
                    "topic=%s partition=%s offset=%s",
                    message.topic(),
                    message.partition(),
                    message.offset(),
                )
                raise

            # Commit only after Neon confirms the transaction.
            consumer.commit(message=message, asynchronous=False)

            logger.debug(
                "Kafka offset committed topic=%s partition=%s offset=%s",
                message.topic(),
                message.partition(),
                message.offset(),
            )

    finally:
        # Release the Kafka consumer even when processing fails.
        consumer.close()
        logger.info("Kafka consumer stopped")


if __name__ == "__main__":
    consume_events()