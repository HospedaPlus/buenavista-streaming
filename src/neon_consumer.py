"""Consume Kafka events and persist them to the Neon event ledger.

Processing order is intentional:
1. Receive and validate a message.
2. Commit the event and Kafka provenance to Neon.
3. Commit the Kafka offset only after the database transaction succeeds.

Messages that fail validation are copied to a dead-letter topic (DLQ) with the
error in a header, and then their offset is committed: one bad message no longer
stops the whole stream, and nothing is lost because the DLQ keeps it.

The consumer does not update F3's operational reservation records yet. The next
step is to call the inventory function (reservar/liberar) inside the same
transaction as the ledger insert. A dedicated consumer group keeps Neon
ingestion independent from validation and analytics.
"""

import logging
import os
import signal
from typing import Any

from confluent_kafka import Consumer, KafkaError, KafkaException, Producer
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
DEFAULT_TOPIC = "reservation.events.v1_3"
DEFAULT_DLQ_TOPIC = "reservation.events.dlq"


def request_shutdown(signum: int, frame: Any) -> None:
    """Request a graceful stop after the current processing operation."""
    del frame
    global shutdown_requested
    shutdown_requested = True
    logger.info("Shutdown requested: signal=%s", signum)


def send_to_dead_letter(producer, dlq_topic: str, message, error: Exception) -> None:
    """Copy an invalid message to the DLQ and wait for the broker to confirm it.

    The original key, value and headers are kept; the error and the original
    position are added as headers so the message can be inspected and replayed.
    """
    headers = list(message.headers() or [])
    headers += [
        ("dlq_error", str(error)[:500].encode("utf-8")),
        ("dlq_source_topic", str(message.topic()).encode("utf-8")),
        ("dlq_source_partition", str(message.partition()).encode("utf-8")),
        ("dlq_source_offset", str(message.offset()).encode("utf-8")),
    ]
    producer.produce(dlq_topic, key=message.key(), value=message.value(), headers=headers)
    remaining = producer.flush(30)
    if remaining:
        raise TimeoutError("dead-letter message was not confirmed by the broker")


def consume_events() -> None:
    """Persist validated events before committing their Kafka offsets."""
    global shutdown_requested
    shutdown_requested = False

    broker = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
    topic = os.getenv("KAFKA_RESERVATION_TOPIC", DEFAULT_TOPIC)
    dlq_topic = os.getenv("KAFKA_DLQ_TOPIC", DEFAULT_DLQ_TOPIC)
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
    dlq_producer = Producer(
        {"bootstrap.servers": broker, "acks": "all", "enable.idempotence": True}
    )
    consumer.subscribe([topic])

    logger.info(
        "Neon consumer started broker=%s topic=%s group=%s",
        broker,
        topic,
        group_id,
    )

    sink = None
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
            except (ValidationError, ValueError) as error:
                logger.warning(
                    "Invalid event sent to DLQ topic=%s partition=%s offset=%s dlq=%s",
                    message.topic(),
                    message.partition(),
                    message.offset(),
                    dlq_topic,
                )
                # The offset is committed only after the DLQ write is confirmed.
                send_to_dead_letter(dlq_producer, dlq_topic, message, error)
                consumer.commit(message=message, asynchronous=False)
                continue

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
        if sink is not None:
            sink.close()
        logger.info("Neon consumer stopped")


if __name__ == "__main__":
    consume_events()
