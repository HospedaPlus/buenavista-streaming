"""Publish validated synthetic reservation events to Apache Kafka.

Two modes are supported:

* Batch (``--count N``): publish N events as fast as possible and stop.
* Continuous (``--continuous``): publish an endless stream whose rate follows a
  profile with a base rate and periodic peaks, so the source demonstrates the F2
  requirement of at least 10 events per second at peak.

Each event is validated before publishing, uses ``reservation_id`` as the Kafka
key (so all events of one reservation stay in the same partition) and every
broker delivery outcome is checked.
"""

import argparse
import logging
import os
import signal
import time
from collections.abc import Callable, Sequence
from dataclasses import dataclass, field

from confluent_kafka import Producer
from confluent_kafka.error import KafkaException

from src.events import (
    DEFAULT_OUT_OF_ORDER_RATE,
    TARGET_OCCUPANCY,
    DemandSurge,
    ReservationEvent,
    generate_events,
)

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
logger = logging.getLogger(__name__)

DEFAULT_TOPIC = "reservation.events.v1_3"
shutdown_requested = False


class KafkaDeliveryError(RuntimeError):
    """Raised when one or more produced Kafka messages fail delivery."""


@dataclass(frozen=True)
class RateProfile:
    """Target publication rate: a base rate with a peak window repeated periodically.

    With the defaults the stream sends 5 events/s and, for the first 10 seconds
    of every minute, 15 events/s.
    """

    base_rate: float = 5.0
    peak_rate: float = 15.0
    peak_every_s: float = 60.0
    peak_duration_s: float = 10.0

    def __post_init__(self):
        if self.base_rate <= 0 or self.peak_rate <= 0:
            raise ValueError("rates must be greater than zero")
        if self.peak_every_s < 0 or self.peak_duration_s < 0:
            raise ValueError("peak window values cannot be negative")

    def rate_at(self, elapsed_s: float) -> float:
        """Return the target events per second at a moment of the run.

        Each cycle starts with its peak window, so a run shows a peak right away.
        """
        if self.peak_every_s > 0 and elapsed_s % self.peak_every_s < self.peak_duration_s:
            return self.peak_rate
        return self.base_rate


@dataclass
class ThroughputMeter:
    """Count events per wall-clock second and keep the observed peak."""

    current_second: int | None = None
    count: int = 0
    peak_per_second: int = 0
    total: int = 0
    _history: list[int] = field(default_factory=list)

    def record(self, now_s: float) -> None:
        second = int(now_s)
        if self.current_second is None:
            self.current_second = second
        if second != self.current_second:
            self._close_second()
            self.current_second = second
        self.count += 1
        self.total += 1

    def _close_second(self) -> None:
        self.peak_per_second = max(self.peak_per_second, self.count)
        self._history.append(self.count)
        self.count = 0

    def summary(self) -> str:
        return f"sent={self.total} peak={self.peak_per_second} ev/s (best full second)"


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


def _produce(producer, topic: str, event: ReservationEvent, on_delivery: Callable) -> None:
    producer.produce(
        topic,
        key=event.reservation_id.encode("utf-8"),
        value=event.model_dump_json().encode("utf-8"),
        headers={
            "event_id": str(event.event_id),
            "event_type": event.event_type,
            "schema_version": event.schema_version,
            "source": event.source,
        },
        on_delivery=on_delivery,
    )
    producer.poll(0)


def publish_events(
    count: int | None = 20,
    seed: int = 42,
    topic: str | None = None,
    bootstrap_servers: str | None = None,
    out_of_order_rate: float = DEFAULT_OUT_OF_ORDER_RATE,
    occupancy: float = TARGET_OCCUPANCY,
    surges: Sequence[DemandSurge] = (),
    rate_profile: RateProfile | None = None,
    duration_s: float | None = None,
    clock: Callable[[], float] = time.monotonic,
    sleep: Callable[[float], None] = time.sleep,
) -> int:
    """Publish schema-validated events to Kafka.

    Without rate_profile the events are sent as fast as possible (batch mode) and
    count is required. With rate_profile the stream is paced; it stops after count
    events, after duration_s seconds, or on SIGINT/SIGTERM, whichever comes first.
    """
    global shutdown_requested
    shutdown_requested = False
    if rate_profile is None and (count is None or count < 1):
        raise ValueError("count must be at least 1 in batch mode")
    if count is not None and count < 1:
        raise ValueError("count must be at least 1")

    broker = bootstrap_servers or os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
    target_topic = topic or os.getenv("KAFKA_RESERVATION_TOPIC", DEFAULT_TOPIC)
    producer = Producer(
        {
            "bootstrap.servers": broker,
            "client.id": "hospedaplus-f2-producer",
            # Wait for all in-sync replicas and avoid duplicates on retry (RNF-05).
            "acks": "all",
            "enable.idempotence": True,
        }
    )
    sent = 0
    delivery_errors: list[str] = []
    meter = ThroughputMeter()

    def record_delivery(error, message) -> None:
        delivery_report(error, message)
        if error is not None:
            delivery_errors.append(str(error))

    start = clock()
    next_send = start
    last_log = start
    events = generate_events(
        seed=seed, out_of_order_rate=out_of_order_rate, occupancy=occupancy, surges=surges
    )
    try:
        for event in events:
            if rate_profile is not None:
                now = clock()
                if shutdown_requested or (duration_s is not None and now - start >= duration_s):
                    break
                if next_send > now:
                    sleep(next_send - now)
                    now = clock()
                next_send = max(next_send, now - 1.0) + 1.0 / rate_profile.rate_at(now - start)
            _produce(producer, target_topic, event, record_delivery)
            sent += 1
            meter.record(clock())
            if delivery_errors:
                raise KafkaDeliveryError(
                    f"{len(delivery_errors)} Kafka message(s) failed delivery; "
                    f"first error: {delivery_errors[0]}"
                )
            if rate_profile is not None and clock() - last_log >= 10:
                last_log = clock()
                logger.info("Streaming progress: %s", meter.summary())
            if count is not None and sent >= count:
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
        "Kafka publication complete: %s topic=%s broker=%s", meter.summary(), target_topic, broker
    )
    return sent


def request_shutdown(signum, frame) -> None:
    """Stop a continuous run after the current event."""
    del frame
    global shutdown_requested
    shutdown_requested = True
    logger.info("Shutdown requested: signal=%s", signum)


def main(argv: list[str] | None = None) -> None:
    """Parse CLI arguments and run a batch or continuous producer."""
    parser = argparse.ArgumentParser(description="Publish synthetic F2 events to Kafka.")
    parser.add_argument("--count", type=int, default=None, help="events to send (batch: 20)")
    parser.add_argument(
        "--seed",
        type=int,
        default=None,
        help="random seed (batch: 42; continuous: new each run so events never repeat)",
    )
    parser.add_argument("--topic", default=None)
    parser.add_argument("--bootstrap-servers", default=None)
    parser.add_argument("--out-of-order-rate", type=float, default=DEFAULT_OUT_OF_ORDER_RATE)
    parser.add_argument(
        "--occupancy",
        type=float,
        default=TARGET_OCCUPANCY,
        help="target chain occupancy used to calibrate the booking rate (default 0.68)",
    )
    parser.add_argument(
        "--surge",
        action="append",
        default=[],
        metavar="HOTEL:START:END:MULT",
        help="extra demand, e.g. HP-PT-001:2026-10-20:2026-10-26:2.0 (repeatable)",
    )
    parser.add_argument("--continuous", action="store_true", help="paced endless stream")
    parser.add_argument("--base-rate", type=float, default=5.0, help="events/s outside peaks")
    parser.add_argument("--peak-rate", type=float, default=15.0, help="events/s during peaks")
    parser.add_argument("--peak-every", type=float, default=60.0, help="seconds between peaks")
    parser.add_argument("--peak-duration", type=float, default=10.0, help="peak length (s)")
    parser.add_argument("--duration", type=float, default=None, help="stop after N seconds")
    args = parser.parse_args(argv)

    seed = args.seed
    if seed is None:
        # A fixed seed in continuous mode replays the same event_ids on every
        # start, and the Neon ledger ignores them as duplicates.
        seed = time.time_ns() % 2**31 if args.continuous else 42
    logger.info("Producer seed=%s", seed)
    surges = [DemandSurge.parse(text) for text in args.surge]
    rate_profile = None
    count = args.count
    if args.continuous:
        rate_profile = RateProfile(
            args.base_rate, args.peak_rate, args.peak_every, args.peak_duration
        )
        signal.signal(signal.SIGINT, request_shutdown)
        signal.signal(signal.SIGTERM, request_shutdown)
    elif count is None:
        count = 20

    publish_events(
        count=count,
        seed=seed,
        topic=args.topic,
        bootstrap_servers=args.bootstrap_servers,
        out_of_order_rate=args.out_of_order_rate,
        occupancy=args.occupancy,
        surges=surges,
        rate_profile=rate_profile,
        duration_s=args.duration,
    )


if __name__ == "__main__":
    main()
