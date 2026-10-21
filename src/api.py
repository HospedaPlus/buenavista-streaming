"""FastAPI entry point for validated reservation events.

The API acknowledges a request only after Kafka confirms message delivery.
Neon persistence is performed asynchronously by the dedicated Kafka consumer.
"""

import logging
import os
from datetime import UTC, datetime
from uuid import uuid4

from confluent_kafka import Producer
from confluent_kafka.error import KafkaException
from fastapi import FastAPI, HTTPException, status
from pydantic import BaseModel, ConfigDict, Field

from src.events import ReservationEvent, ReservationPayload

logger = logging.getLogger(__name__)
app = FastAPI(
    title="Buenavista Reservation Streaming API",
    version="0.1.0",
    description="Accepts reservation events and publishes them to Apache Kafka.",
)


class CreateReservationRequest(BaseModel):
    """Input contract for a newly created synthetic or upstream reservation."""

    model_config = ConfigDict(extra="forbid")

    hotel_id: str = Field(pattern=r"^HP-[A-Z]{2}-\d{3}$")
    reservation_id: str = Field(min_length=1, max_length=80)
    data: ReservationPayload


class PublishResult(BaseModel):
    """HTTP response returned after Kafka confirms the event."""

    status: str
    event_id: str
    topic: str


def publish_to_kafka(event: ReservationEvent) -> str:
    """Publish one validated event and wait for its broker acknowledgement."""
    broker = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
    topic = os.getenv("KAFKA_RESERVATION_TOPIC", "reservation.events.v1_2")
    producer = Producer(
        {
            "bootstrap.servers": broker,
            "client.id": "buenavista-fastapi",
            "message.timeout.ms": 10000,
        }
    )
    delivery_errors: list[str] = []

    def on_delivery(error, message) -> None:
        if error is not None:
            delivery_errors.append(str(error))
            logger.error("Kafka delivery failed: %s", error)
        else:
            logger.info(
                "Kafka delivery confirmed event_id=%s topic=%s partition=%s offset=%s",
                event.event_id,
                message.topic(),
                message.partition(),
                message.offset(),
            )

    try:
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
        remaining = producer.flush(10)
        if remaining:
            raise TimeoutError(f"{remaining} Kafka message(s) were not delivered before timeout")
        if delivery_errors:
            raise RuntimeError(delivery_errors[0])
    except (KafkaException, BufferError, TimeoutError, RuntimeError) as exc:
        logger.exception("Could not publish event_id=%s", event.event_id)
        raise HTTPException(
            status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
            detail="Kafka did not confirm event delivery; retry the request safely.",
        ) from exc

    return topic


@app.get("/health", tags=["health"])
def health() -> dict[str, str]:
    """Expose process health without claiming Kafka or Neon connectivity."""
    return {"status": "ok"}


@app.post(
    "/reservations",
    response_model=PublishResult,
    status_code=status.HTTP_202_ACCEPTED,
    tags=["reservations"],
)
def create_reservation(request: CreateReservationRequest) -> PublishResult:
    """Create a version-1 reservation event and publish it to Kafka."""
    event = ReservationEvent(
        event_id=uuid4(),
        schema_version="1.2",
        source="f2-channel-manager",
        event_type="reservation.created",
        occurred_at=datetime.now(UTC),
        hotel_id=request.hotel_id,
        reservation_id=request.reservation_id,
        reservation_version=1,
        data=request.data,
    )
    topic = publish_to_kafka(event)
    return PublishResult(status="accepted_by_kafka", event_id=str(event.event_id), topic=topic)


@app.post(
    "/events",
    response_model=PublishResult,
    status_code=status.HTTP_202_ACCEPTED,
    tags=["events"],
)
def ingest_event(event: ReservationEvent) -> PublishResult:
    """Accept an existing event from the synthetic seed and forward it to Kafka."""
    topic = publish_to_kafka(event)
    return PublishResult(status="accepted_by_kafka", event_id=str(event.event_id), topic=topic)
