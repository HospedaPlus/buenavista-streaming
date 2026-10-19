"""Independent OLAP Kafka group that captures raw events to S3 Bronze."""
import logging
import signal
from confluent_kafka import Consumer, KafkaError, KafkaException, TopicPartition

from src.bronze import BronzeWriter
from src.config import get_settings

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s %(message)s")
logger = logging.getLogger(__name__)
shutdown = False


def request_shutdown(signum, _frame):
    global shutdown
    shutdown = True
    logger.info("Shutdown requested (signal %s)", signum)


def consume_events() -> None:
    settings = get_settings()
    writer = BronzeWriter(settings)
    consumer = Consumer(
        {
            "bootstrap.servers": settings.bootstrap_servers,
            "security.protocol": settings.security_protocol,
            "group.id": settings.bronze_consumer_group,
            "client.id": "hospedaplus-f2-bronze-capture",
            "enable.auto.commit": False,
            "enable.auto.offset.store": False,
            "auto.offset.reset": "earliest",
            "allow.auto.create.topics": False,
        }
    )
    consumer.subscribe([settings.topic])
    logger.info(
        "OLAP Bronze capture group=%s topic=%s",
        settings.bronze_consumer_group,
        settings.topic,
    )
    try:
        while not shutdown:
            messages = consumer.consume(
                num_messages=settings.bronze_batch_size,
                timeout=settings.bronze_batch_timeout_seconds,
            )
            records = [message for message in messages if message is not None]
            for message in records:
                if message.error() and message.error().code() != KafkaError._PARTITION_EOF:
                    raise KafkaException(message.error())
            records = [message for message in records if not message.error()]
            if not records:
                continue

            # Persist every grouped S3 object before committing any offset in this batch.
            keys = writer.write_batch(records)
            highest_offsets = {}
            for message in records:
                key = (message.topic(), message.partition())
                highest_offsets[key] = max(
                    highest_offsets.get(key, -1), message.offset()
                )
            offsets = [
                TopicPartition(topic, partition, offset + 1)
                for (topic, partition), offset in highest_offsets.items()
            ]
            consumer.commit(offsets=offsets, asynchronous=False)
            logger.info(
                "Bronze batch persisted records=%s objects=%s",
                len(records),
                len(keys),
            )
    finally:
        consumer.close()


def main() -> None:
    signal.signal(signal.SIGINT, request_shutdown)
    signal.signal(signal.SIGTERM, request_shutdown)
    consume_events()


if __name__ == "__main__":
    main()
