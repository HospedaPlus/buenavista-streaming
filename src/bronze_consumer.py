"""Independent OLAP Kafka group that captures raw events to S3 Bronze."""
import logging
import signal
from confluent_kafka import Consumer, KafkaError, KafkaException
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
    consumer = Consumer({
        "bootstrap.servers": settings.bootstrap_servers,
        "security.protocol": settings.security_protocol,
        "group.id": settings.bronze_consumer_group,
        "client.id": "hospedaplus-f2-bronze-capture",
        "enable.auto.commit": False,
        "enable.auto.offset.store": False,
        "auto.offset.reset": "earliest",
        "allow.auto.create.topics": False,
    })
    consumer.subscribe([settings.topic])
    logger.info("OLAP Bronze capture group=%s topic=%s",
                settings.bronze_consumer_group, settings.topic)
    try:
        while not shutdown:
            msg = consumer.poll(1.0)
            if msg is None:
                continue
            if msg.error():
                if msg.error().code() == KafkaError._PARTITION_EOF:
                    continue
                raise KafkaException(msg.error())
            timestamp = msg.timestamp()
            kafka_timestamp_ms = timestamp[1] if timestamp and timestamp[0] >= 0 else None
            # Preserve the original Kafka value plus ingestion metadata and coordinates.
            key = writer.write(msg.value(), msg.topic(), msg.partition(), msg.offset(),
                               kafka_timestamp_ms)
            consumer.commit(message=msg, asynchronous=False)
            logger.info("Bronze record persisted key=%s", key)
    finally:
        consumer.close()


def main() -> None:
    signal.signal(signal.SIGINT, request_shutdown)
    signal.signal(signal.SIGTERM, request_shutdown)
    consume_events()


if __name__ == "__main__":
    main()
