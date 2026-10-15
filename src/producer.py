"""Publish synthetic F2 reservation events to a durable RabbitMQ queue.

This producer uses persistent messages and publisher confirms. That confirms broker
acceptance, not downstream database persistence or exactly-once processing.
"""

import argparse
import logging

import pika
from pika.exceptions import AMQPConnectionError, NackError, UnroutableError

from src.config import get_settings
from src.events import build_event

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
logger = logging.getLogger(__name__)


def create_connection() -> pika.BlockingConnection:
    """Open an authenticated AMQP connection using the current configuration."""
    settings = get_settings()
    credentials = pika.PlainCredentials(settings.username, settings.password)
    parameters = pika.ConnectionParameters(
        host=settings.host,
        port=settings.port,
        virtual_host=settings.virtual_host,
        credentials=credentials,
        heartbeat=30,
        blocked_connection_timeout=30,
        connection_attempts=3,
        retry_delay=2,
    )
    return pika.BlockingConnection(parameters)


def publish_events(count: int, seed: int = 42) -> int:
    """Publish count validated JSON events and return broker-confirmed message count."""
    if count < 1:
        raise ValueError("count must be at least 1")

    settings = get_settings()
    connection = create_connection()
    published = 0
    try:
        channel = connection.channel()
        channel.queue_declare(queue=settings.queue_name, durable=True)
        channel.confirm_delivery()

        for index in range(count):
            event = build_event(index, seed)
            body = event.model_dump_json().encode("utf-8")
            accepted = channel.basic_publish(
                exchange="",
                routing_key=settings.queue_name,
                body=body,
                properties=pika.BasicProperties(
                    content_type="application/json",
                    content_encoding="utf-8",
                    delivery_mode=2,
                    message_id=str(event.event_id),
                    type=event.event_type,
                    timestamp=int(event.occurred_at.timestamp()),
                    headers={"schema_version": event.schema_version, "source": event.source},
                ),
                mandatory=True,
            )
            if accepted is False:
                raise RuntimeError(f"RabbitMQ did not confirm event {event.event_id}")
            published += 1
            logger.info(
                "published event_id=%s event_type=%s hotel_id=%s queue=%s",
                event.event_id,
                event.event_type,
                event.hotel_id,
                settings.queue_name,
            )

        logger.info("publish complete: %d/%d events confirmed by queue %s", published, count, settings.queue_name)
        return published
    except (AMQPConnectionError, NackError, UnroutableError):
        logger.exception("RabbitMQ publish failed")
        raise
    finally:
        if connection.is_open:
            connection.close()


def main() -> None:
    """Parse command-line options and publish a controlled event batch."""
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--count", type=int, default=20, help="Number of synthetic events to publish")
    parser.add_argument("--seed", type=int, default=42, help="Seed for reproducible test events")
    args = parser.parse_args()
    publish_events(args.count, args.seed)


if __name__ == "__main__":
    main()
