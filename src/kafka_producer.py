"""Publish validated reservation events to Apache Kafka.

This is the Kafka migration path. The existing RabbitMQ producer remains
available until the Kafka path has been tested and approved.
"""

import argparse
import logging
import os

from confluent_kafka import Producer
from confluent_kafka.error import KafkaException

from src.events import DEFAULT_OUT_OF_ORDER_RATE, generate_events

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
logger = logging.getLogger(__name__)


class KafkaDeliveryError(RuntimeError):
    """Raised when one or more produced Kafka messages fail delivery."""


def delivery_report(error, message) -> None:
    """Log broker delivery outcomes for observability."""
    if error is not None:
        logger.error("Kafka delivery failed: %s", error)
        return
    logger.debug(
        "Kafka delivery confirmed topic=%s partition=%s offset=%s",
        message.topic(),
        message.partition(),
        message.offset(),
    )


def publish_events(
    count: int = 20,
    seed: int = 42,
    topic: str | None = None,
    bootstrap_servers: str | None = None,
) -> int:
    """Publish a finite number of schema-validated events to Kafka."""
    if count < 1:
        raise ValueError("count must be at least 1")

    broker = bootstrap_servers or os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
    target_topic = topic or os.getenv("KAFKA_RESERVATION_TOPIC", "reservation.events.v1_2")
    producer = Producer({"bootstrap.servers": broker, "client.id": "buenavista-seed-producer"})
    sent = 0
    delivery_errors: list[str] = []

    def record_delivery(error, message) -> None:
        delivery_report(error, message)
        if error is not None:
            delivery_errors.append(str(error))

    try:
        for event in generate_events(seed=seed, out_of_order_rate=DEFAULT_OUT_OF_ORDER_RATE):
            producer.produce(
                target_topic,
                key=event.reservation_id.encode("utf-8"),
                value=event.model_dump_json().encode("utf-8"),
                headers={
                    "event_id": str(event.event_id),
                    "event_type": event.event_type,
                    "schema_version": event.schema_version,
                    "source": event.source,
                },
                on_delivery=record_delivery,
            )
            producer.poll(0)
            sent += 1
            if sent >= count:
                break
        remaining = producer.flush(30)
        if remaining:
            raise TimeoutError(f"{remaining} Kafka message(s) were not confirmed before timeout")
        if delivery_errors:
            raise KafkaDeliveryError(
                f"{len(delivery_errors)} Kafka message(s) failed delivery; "
                f"first error: {delivery_errors[0]}"
            )
    except (KafkaException, BufferError, TimeoutError, KafkaDeliveryError):
        logger.exception("Kafka publication did not complete successfully")
        raise

    logger.info(
        "Kafka publication complete: sent=%s topic=%s broker=%s", sent, target_topic, broker
    )
    return sent


def main() -> None:
    """Parse CLI arguments for a bounded producer run."""
    parser = argparse.ArgumentParser(description="Publish synthetic events to Kafka.")
    parser.add_argument("--count", type=int, default=20)
    parser.add_argument("--seed", type=int, default=42)
    parser.add_argument("--topic", default=None)
    parser.add_argument("--bootstrap-servers", default=None)
    args = parser.parse_args()
    publish_events(args.count, args.seed, args.topic, args.bootstrap_servers)


if __name__ == "__main__":
    main()
