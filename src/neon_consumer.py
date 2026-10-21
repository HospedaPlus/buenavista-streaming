"""Consume Kafka events and persist them to the Neon event ledger.

Processing order is intentional:
1. Receive and validate a message.
2. Commit the event and Kafka provenance to Neon.
3. Commit the Kafka offset only after the database transaction succeeds.

The consumer does not update F3's operational reservation records. A dedicated
consumer group keeps Neon ingestion independent from validation and analytics.
"""

import logging
import os
import signal
from typing import Any

from confluent_kafka import Consumer, KafkaError, KafkaException
from pydantic import ValidationError

from src.events import ReservationEvent
from src.neon_sink import NeonEventSink

logging.basicConfig(
    level=os.getenv("LOG_LEVEL", "INFO").upper(),
    format="%(asctime)s %(levelname)s %(name)s %(message)s",
)
logger = logging.getLogger(__name__)
shutdown_requested = False
POLL_TIMEOUT_SECONDS = 1.0


def request_shutdown(signum: int, frame: Any) -> None:
    """Request a graceful stop after the current processing operation."""
    del frame
    global shutdown_requested
    shutdown_requested = True
    logger.info("Shutdown requested: signal=%s", signum)


def consume_events() -> None:
    """Persist validated events before committing their Kafka offsets."""
    global shutdown_requested
    shutdown_requested = False

    broker = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
    topic = os.getenv("KAFKA_RESERVATION_TOPIC", "reservation.events.v1_2")
    group_id = os.getenv("KAFKA_NEON_GROUP_ID", "buenavista-neon-ledger")

    # Automatic commits are disabled: offsets must follow durable Neon writes.
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
        "Neon consumer started broker=%s topic=%s group=%s",
        broker,
        topic,
        group_id,
    )

    try:
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
                event = ReservationEvent.model_validate_json(message.value())
            except (ValidationError, ValueError):
                logger.exception(
                    "Invalid event; offset remains uncommitted topic=%s partition=%s offset=%s",
                    message.topic(),
                    message.partition(),
                    message.offset(),
                )
                # Do not skip invalid data. A Kafka DLQ must be implemented and
                # configured before this consumer is used in production.
                raise

            # If this transaction fails, the following commit is never reached.
            inserted = sink.persist(
                event=event,
                topic=message.topic(),
                partition=message.partition(),
                offset=message.offset(),
            )
            consumer.commit(message=message, asynchronous=False)

            logger.info(
                "Neon ledger write confirmed event_id=%s inserted=%s topic=%s "
                "partition=%s offset=%s",
                event.event_id,
                inserted,
                message.topic(),
                message.partition(),
                message.offset(),
            )
    finally:
        consumer.close()
        logger.info("Neon consumer stopped")


if __name__ == "__main__":
    consume_events()
