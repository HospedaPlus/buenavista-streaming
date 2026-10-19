"""Neon OLTP reservation projection consumer (independent Kafka group)."""
import json
import logging
import signal
import time
from confluent_kafka import Consumer, KafkaError, KafkaException, Producer
from pydantic import ValidationError
from src.config import get_settings
from src.events import ReservationEvent
from src.sinks import NeonProjector

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s %(message)s")
logger = logging.getLogger(__name__)
shutdown = False

def request_shutdown(signum, _frame):
    global shutdown
    shutdown = True
    logger.info("Shutdown requested (signal %s)", signum)

def _header_map(message):
    return dict(message.headers() or [])

def _retry_count(message):
    raw = _header_map(message).get("retry_count", b"0")
    return int(raw.decode("ascii") if isinstance(raw, bytes) else raw)

def _forward(producer, topic, message, reason, retry_count=None):
    headers = [(key, value) for key, value in (message.headers() or []) if key != "retry_count"]
    headers.append(("processing_error", str(reason)[:500].encode("utf-8")))
    if retry_count is not None:
        headers.append(("retry_count", str(retry_count).encode("ascii")))
    result = []
    producer.produce(topic, key=message.key(), value=message.value(), headers=headers,
                     on_delivery=lambda error, _record: result.append(error))
    if producer.flush(30):
        raise TimeoutError(f"Timed out forwarding record to {topic}")
    if result and result[0] is not None:
        raise KafkaException(result[0])

def consume_events() -> None:
    settings = get_settings()
    projector = NeonProjector(settings)
    consumer = Consumer({
        "bootstrap.servers": settings.bootstrap_servers,
        "security.protocol": settings.security_protocol,
        "group.id": settings.neon_consumer_group,
        "client.id": "hospedaplus-f2-neon-projector",
        "enable.auto.commit": False,
        "enable.auto.offset.store": False,
        "auto.offset.reset": "earliest",
        "allow.auto.create.topics": False,
    })
    forwarder = Producer({"bootstrap.servers": settings.bootstrap_servers,
                          "security.protocol": settings.security_protocol,
                          "client.id": "hospedaplus-f2-neon-retry",
                          "acks": "all", "enable.idempotence": True})
    consumer.subscribe([settings.topic, settings.retry_topic])
    logger.info("OLTP consumer group=%s topics=%s,%s", settings.neon_consumer_group,
                settings.topic, settings.retry_topic)
    try:
        while not shutdown:
            msg = consumer.poll(1.0)
            if msg is None:
                continue
            if msg.error():
                if msg.error().code() == KafkaError._PARTITION_EOF:
                    continue
                raise KafkaException(msg.error())
            is_retry = msg.topic() == settings.retry_topic
            try:
                event = ReservationEvent.model_validate_json(msg.value())
            except (UnicodeDecodeError, ValidationError) as exc:
                _forward(forwarder, settings.dlq_topic, msg, exc)
                consumer.commit(message=msg, asynchronous=False)
                logger.warning("Invalid event routed to %s", settings.dlq_topic)
                continue
            try:
                if is_retry:
                    time.sleep(settings.retry_delay_seconds)
                projector.apply(event)
            except Exception as exc:
                attempts = _retry_count(msg)
                if attempts < settings.max_retries:
                    _forward(forwarder, settings.retry_topic, msg, exc, attempts + 1)
                    consumer.commit(message=msg, asynchronous=False)
                    logger.warning("Scheduled retry %s/%s event_id=%s",
                                   attempts + 1, settings.max_retries, event.event_id)
                else:
                    _forward(forwarder, settings.dlq_topic, msg, exc)
                    consumer.commit(message=msg, asynchronous=False)
                    logger.exception("Retry limit exhausted; sent event_id=%s to DLQ", event.event_id)
                continue
            consumer.commit(message=msg, asynchronous=False)
            logger.info("OLTP transaction committed event_id=%s reservation_id=%s version=%s",
                        event.event_id, event.reservation_id, event.reservation_version)
    finally:
        consumer.close()
        forwarder.flush(10)
        projector.close()

def main() -> None:
    signal.signal(signal.SIGINT, request_shutdown)
    signal.signal(signal.SIGTERM, request_shutdown)
    consume_events()

if __name__ == "__main__":
    main()
