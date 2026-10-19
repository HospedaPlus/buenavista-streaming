"""Consume Kafka events and persist them to Neon and the F2 Bronze layer."""
import json
import logging
import signal
from confluent_kafka import Consumer, KafkaError, KafkaException, Producer
from pydantic import ValidationError
from src.config import get_settings
from src.events import ReservationEvent
from src.sinks import EventSinks

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s %(message)s")
logger = logging.getLogger(__name__)
shutdown = False

def request_shutdown(signum, _frame):
    global shutdown
    shutdown = True
    logger.info("Shutdown requested (signal %s)", signum)

def consume_events() -> None:
    settings = get_settings()
    sinks = EventSinks(settings)
    consumer = Consumer({
        "bootstrap.servers": settings.bootstrap_servers,
        "security.protocol": settings.security_protocol,
        "group.id": settings.consumer_group,
        "client.id": "hospedaplus-f2-neon-bronze-consumer",
        "enable.auto.commit": False,
        "enable.auto.offset.store": False,
        "auto.offset.reset": "earliest",
        "allow.auto.create.topics": False,
    })
    dlq = Producer({
        "bootstrap.servers": settings.bootstrap_servers,
        "security.protocol": settings.security_protocol,
        "client.id": "hospedaplus-f2-dlq-producer",
        "acks": "all",
        "enable.idempotence": True,
    })
    consumer.subscribe([settings.topic])
    logger.info("Consuming topic=%s group=%s", settings.topic, settings.consumer_group)
    try:
        while not shutdown:
            msg = consumer.poll(1.0)
            if msg is None:
                continue
            if msg.error():
                if msg.error().code() == KafkaError._PARTITION_EOF:
                    continue
                raise KafkaException(msg.error())
            try:
                payload = json.loads(msg.value().decode("utf-8"))
                event = ReservationEvent.model_validate(payload)
            except (UnicodeDecodeError, json.JSONDecodeError, ValidationError) as exc:
                headers = list(msg.headers() or [])
                headers.append(("dead_letter_reason", str(exc)[:500].encode("utf-8")))
                delivered = []
                dlq.produce(settings.dlq_topic, key=msg.key(), value=msg.value(),
                            headers=headers, on_delivery=lambda error, _message: delivered.append(error))
                if dlq.flush(30):
                    raise TimeoutError("Kafka DLQ delivery timed out")
                if delivered and delivered[0] is not None:
                    raise KafkaException(delivered[0])
                consumer.commit(message=msg, asynchronous=False)
                logger.warning("Invalid record sent to %s at %s[%s]@%s",
                               settings.dlq_topic, msg.topic(), msg.partition(), msg.offset())
                continue
            # Sink writes are idempotent. Commit only after both sinks succeed;
            # a crash can replay an event but cannot overwrite another event.
            sinks.write(event)
            consumer.commit(message=msg, asynchronous=False)
            logger.info("Persisted event_id=%s hotel_id=%s type=%s version=%s",
                        event.event_id, event.hotel_id, event.event_type, event.reservation_version)
    finally:
        consumer.close()
        dlq.flush(10)

def main() -> None:
    signal.signal(signal.SIGINT, request_shutdown)
    signal.signal(signal.SIGTERM, request_shutdown)
    try:
        consume_events()
    except Exception:
        logger.exception("Kafka consumer stopped after an uncommitted processing error")
        raise

if __name__ == "__main__":
    main()
