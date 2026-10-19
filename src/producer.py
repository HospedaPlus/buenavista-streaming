"""Publish synthetic F2 reservation lifecycle events to Kafka."""
import argparse
import logging
import math
import signal
import time
from dataclasses import dataclass

from confluent_kafka import Producer

from src.config import get_settings
from src.events import DEFAULT_OUT_OF_ORDER_RATE, generate_events

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
logger = logging.getLogger(__name__)


@dataclass(frozen=True)
class RateProfile:
    base_rate: float = 5.0
    peak_rate: float = 15.0
    peak_every_s: float = 60.0
    peak_duration_s: float = 10.0

    def __post_init__(self) -> None:
        if self.base_rate <= 0 or self.peak_rate <= 0:
            raise ValueError("rates must be greater than zero")
        if self.peak_every_s < 0 or self.peak_duration_s < 0:
            raise ValueError("peak window values cannot be negative")

    def rate_at(self, elapsed_s: float) -> float:
        if self.peak_every_s and elapsed_s % self.peak_every_s < self.peak_duration_s:
            return self.peak_rate
        return self.base_rate


def publish_events(count: int | None = 20, seed: int = 42,
                   profile: RateProfile | None = None, duration_s: float | None = None,
                   out_of_order_rate: float = DEFAULT_OUT_OF_ORDER_RATE) -> int:
    if count is not None and count < 1:
        raise ValueError("count must be at least 1")
    if duration_s is not None and duration_s <= 0:
        raise ValueError("duration must be greater than zero")

    settings = get_settings()
    delivered = 0
    failures: list[str] = []
    producer = Producer({
        "bootstrap.servers": settings.bootstrap_servers,
        **settings.kafka_security_config(),
        "client.id": "hospedaplus-f2-producer",
        "acks": "all",
        "enable.idempotence": True,
        "message.timeout.ms": 30000,
    })

    def on_delivery(error, message):
        nonlocal delivered
        if error:
            failures.append(str(error))
            logger.error("Kafka delivery failed: %s", error)
        else:
            delivered += 1

    started = time.monotonic()
    next_send_at = started
    scheduled = 0
    stream = generate_events(seed, out_of_order_rate)
    try:
        while count is None or scheduled < count:
            now = time.monotonic()
            elapsed = now - started
            if duration_s is not None and elapsed >= duration_s:
                break
            target = profile.rate_at(elapsed) if profile else math.inf
            if profile:
                if next_send_at > now:
                    producer.poll(min(next_send_at - now, 0.25))
                    continue
                if now - next_send_at > 1:
                    next_send_at = now
                next_send_at += 1 / target

            event = next(stream)
            while True:
                try:
                    producer.produce(
                        topic=settings.topic,
                        key=event.reservation_id.encode("utf-8"),
                        value=event.model_dump_json().encode("utf-8"),
                        headers=[
                            ("event_id", str(event.event_id).encode()),
                            ("event_type", event.event_type.encode()),
                            ("schema_version", event.schema_version.encode()),
                            ("source", event.source.encode()),
                        ],
                        on_delivery=on_delivery,
                    )
                    break
                except BufferError:
                    producer.poll(0.1)
            scheduled += 1
            producer.poll(0)

        remaining = producer.flush(30)
        if remaining:
            raise TimeoutError(f"{remaining} Kafka message(s) were not delivered before timeout")
        if failures:
            raise RuntimeError(failures[0])
        logger.info(
            "publish complete: scheduled=%d delivered=%d topic=%s",
            scheduled,
            delivered,
            settings.topic,
        )
        return delivered
    except KeyboardInterrupt:
        producer.flush(30)
        logger.info("publish stopped: scheduled=%d delivered=%d", scheduled, delivered)
        return delivered


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--count", type=int)
    parser.add_argument("--continuous", action="store_true")
    parser.add_argument("--duration", type=float)
    parser.add_argument("--seed", type=int, default=42)
    parser.add_argument("--rate", type=float, default=5.0)
    parser.add_argument("--peak-rate", type=float, default=15.0)
    parser.add_argument("--peak-every", type=float, default=60.0)
    parser.add_argument("--peak-duration", type=float, default=10.0)
    parser.add_argument("--out-of-order-rate", type=float, default=DEFAULT_OUT_OF_ORDER_RATE)
    return parser


def main() -> None:
    args = build_parser().parse_args()
    signal.signal(signal.SIGTERM, signal.default_int_handler)
    profile = (
        RateProfile(args.rate, args.peak_rate, args.peak_every, args.peak_duration)
        if args.continuous
        else None
    )
    publish_events(
        args.count if args.continuous else (args.count or 20),
        args.seed,
        profile,
        args.duration,
        args.out_of_order_rate,
    )


if __name__ == "__main__":
    main()
