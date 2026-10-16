"""Publish synthetic F2 reservation events to RabbitMQ, in batches or as a continuous stream.

Events go to a durable topic exchange with the event type as routing key and
are delivered to a durable queue bound to it. The producer uses persistent
messages and publisher confirms. That confirms broker acceptance, not
downstream database persistence or exactly-once processing.

In continuous mode the publication rate follows a profile with a base rate and
periodic peaks, so the source can demonstrate the F2 requirement of at least
10 events per second at peak.
"""

import argparse
import logging
import math
import signal
import time
from dataclasses import dataclass, field

import pika
from pika.exceptions import AMQPConnectionError, NackError, UnroutableError

from src.config import get_settings
from src.events import DEFAULT_OUT_OF_ORDER_RATE, generate_events

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
# pika logs every connection step at INFO; keep the producer output readable.
logging.getLogger("pika").setLevel(logging.WARNING)
logger = logging.getLogger(__name__)

# Routing pattern that binds the main queue to every reservation event type.
ALL_RESERVATION_EVENTS = "reservation.#"


@dataclass(frozen=True)
class RateProfile:
    """Target publication rate: a base rate with a peak window repeated periodically."""

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
        """Return the target events per second at a moment of the run.

        Each cycle starts with its peak window, so a run shows a peak right away.
        """
        if self.peak_every_s > 0 and elapsed_s % self.peak_every_s < self.peak_duration_s:
            return self.peak_rate
        return self.base_rate


@dataclass
class ThroughputMeter:
    """Count confirmed events per wall-clock second and keep the observed peak."""

    started_at: float = field(default_factory=time.monotonic)
    total: int = 0
    current_second: int = 0
    current_count: int = 0
    peak_per_second: int = 0
    completed_seconds: list[int] = field(default_factory=list)

    def record(self, now: float | None = None) -> int | None:
        """Count one event; return the finished second's count when a second rolls over."""
        now = time.monotonic() if now is None else now
        second = int(now - self.started_at)
        finished = None
        if second != self.current_second:
            finished = self.current_count
            self._close_second(self.current_count)
            # Seconds without any event count as zero.
            for _ in range(second - self.current_second - 1):
                self._close_second(0)
            self.current_second = second
            self.current_count = 0
        self.current_count += 1
        self.total += 1
        return finished

    def _close_second(self, count: int) -> None:
        self.completed_seconds.append(count)
        self.peak_per_second = max(self.peak_per_second, count)

    def summary(self, now: float | None = None) -> str:
        now = time.monotonic() if now is None else now
        elapsed = max(now - self.started_at, 1e-9)
        return (
            f"total={self.total} elapsed={elapsed:.1f}s average={self.total / elapsed:.1f} ev/s "
            f"peak={self.peak_per_second} ev/s (best full second)"
        )


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


def declare_stream_topology(channel) -> None:
    """Declare the durable topic exchange, the main queue, and their binding.

    All declarations are idempotent, so the producer and the consumer can both
    call this safely in any start-up order.
    """
    settings = get_settings()
    channel.exchange_declare(
        exchange=settings.exchange_name,
        exchange_type="topic",
        durable=True,
    )
    channel.queue_declare(queue=settings.queue_name, durable=True)
    channel.queue_bind(
        queue=settings.queue_name,
        exchange=settings.exchange_name,
        routing_key=ALL_RESERVATION_EVENTS,
    )


def publish_events(
    count: int | None = 20,
    seed: int = 42,
    profile: RateProfile | None = None,
    duration_s: float | None = None,
    out_of_order_rate: float = DEFAULT_OUT_OF_ORDER_RATE,
) -> int:
    """Publish validated JSON events and return the broker-confirmed message count.

    Args:
        count: Number of events to publish; None publishes until stopped.
        seed: Seed for the reproducible event stream.
        profile: Target rate profile; None publishes as fast as the broker confirms.
        duration_s: Optional wall-clock limit in seconds.
        out_of_order_rate: Share of events delivered out of order.
    """
    if count is not None and count < 1:
        raise ValueError("count must be at least 1")
    if duration_s is not None and duration_s <= 0:
        raise ValueError("duration must be greater than zero")

    settings = get_settings()
    connection = create_connection()
    meter = ThroughputMeter()
    try:
        channel = connection.channel()
        declare_stream_topology(channel)
        channel.confirm_delivery()

        stream = generate_events(seed, out_of_order_rate)
        next_send_at = meter.started_at

        while count is None or meter.total < count:
            now = time.monotonic()
            elapsed = now - meter.started_at
            if duration_s is not None and elapsed >= duration_s:
                break

            target = profile.rate_at(elapsed) if profile else math.inf
            if profile:
                if next_send_at > now:
                    # connection.sleep keeps heartbeats flowing while pacing.
                    connection.sleep(next_send_at - now)
                elif now - next_send_at > 1:
                    # Do not burst to catch up after a stall longer than a second.
                    next_send_at = now
                next_send_at += 1 / target

            event = next(stream)
            body = event.model_dump_json().encode("utf-8")
            # With confirms enabled, basic_publish raises NackError or
            # UnroutableError when the broker does not accept the message.
            channel.basic_publish(
                exchange=settings.exchange_name,
                routing_key=event.event_type,
                body=body,
                properties=pika.BasicProperties(
                    content_type="application/json",
                    content_encoding="utf-8",
                    delivery_mode=2,
                    message_id=str(event.event_id),
                    type=event.event_type,
                    timestamp=int(time.time()),
                    headers={"schema_version": event.schema_version, "source": event.source},
                ),
                mandatory=True,
            )
            logger.debug(
                "published event_id=%s event_type=%s hotel_id=%s reservation_id=%s version=%s",
                event.event_id,
                event.event_type,
                event.hotel_id,
                event.reservation_id,
                event.reservation_version,
            )

            finished = meter.record()
            if finished is not None and profile:
                logger.info(
                    "throughput second=%d events=%d target=%.0f ev/s total=%d",
                    meter.current_second - 1,
                    finished,
                    profile.rate_at(meter.current_second - 1),
                    meter.total,
                )

        logger.info("publish complete: %s exchange=%s", meter.summary(), settings.exchange_name)
        return meter.total
    except KeyboardInterrupt:
        logger.info("publish stopped by user: %s", meter.summary())
        return meter.total
    except (AMQPConnectionError, NackError, UnroutableError):
        logger.exception("RabbitMQ publish failed after %d confirmed events", meter.total)
        raise
    finally:
        if connection.is_open:
            connection.close()


def build_parser() -> argparse.ArgumentParser:
    """Describe the command-line options for batch and continuous publication."""
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    parser.add_argument("--count", type=int, help="Stop after this many events")
    parser.add_argument(
        "--continuous",
        action="store_true",
        help="Publish continuously with the rate profile until Ctrl+C or --duration",
    )
    parser.add_argument("--duration", type=float, help="Stop after this many seconds")
    parser.add_argument("--seed", type=int, default=42, help="Seed for reproducible events")
    parser.add_argument("--rate", type=float, default=5.0, help="Base events per second")
    parser.add_argument("--peak-rate", type=float, default=15.0, help="Peak events per second")
    parser.add_argument(
        "--peak-every", type=float, default=60.0, help="Seconds between peak starts (0 disables)"
    )
    parser.add_argument(
        "--peak-duration", type=float, default=10.0, help="Length of each peak in seconds"
    )
    parser.add_argument(
        "--out-of-order-rate",
        type=float,
        default=DEFAULT_OUT_OF_ORDER_RATE,
        help="Share of events delivered out of order (0 to 1)",
    )
    parser.add_argument("--verbose", action="store_true", help="Log every published event")
    return parser


def main() -> None:
    """Parse command-line options and publish a batch or a continuous stream."""
    args = build_parser().parse_args()
    if args.verbose:
        logger.setLevel(logging.DEBUG)

    # Treat SIGTERM (docker stop, ECS task stop) like Ctrl+C for a clean summary.
    signal.signal(signal.SIGTERM, signal.default_int_handler)

    if args.continuous:
        profile = RateProfile(args.rate, args.peak_rate, args.peak_every, args.peak_duration)
        count = args.count
    else:
        # Batch mode keeps the original behaviour: publish quickly and stop.
        profile = None
        count = args.count if args.count is not None else 20

    publish_events(
        count=count,
        seed=args.seed,
        profile=profile,
        duration_s=args.duration,
        out_of_order_rate=args.out_of_order_rate,
    )


if __name__ == "__main__":
    main()
