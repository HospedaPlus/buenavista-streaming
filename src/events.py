"""Versioned, PII-minimized event contract for the synthetic F2 source."""

import json
import random
from datetime import UTC, date, datetime, timedelta
from pathlib import Path
from typing import Literal
from uuid import NAMESPACE_URL, UUID, uuid5

from pydantic import BaseModel, ConfigDict, Field, field_validator

EventType = Literal[
    "reservation.created",
    "reservation.updated",
    "reservation.cancelled",
]

CATALOG_PATH = Path(__file__).parent / "catalog" / "hotel_ids.json"
HOTEL_IDS = json.loads(CATALOG_PATH.read_text(encoding="utf-8"))


class ReservationPayload(BaseModel):
    """Minimal reservation facts; intentionally excludes direct guest identifiers."""

    model_config = ConfigDict(extra="forbid")

    check_in: str = Field(pattern=r"^\d{4}-\d{2}-\d{2}$")
    check_out: str = Field(pattern=r"^\d{4}-\d{2}-\d{2}$")
    room_type: str = Field(min_length=1, max_length=80)
    guests: int = Field(ge=1, le=20)
    total_amount: float = Field(ge=0)
    currency: str = Field(pattern=r"^[A-Z]{3}$")
    status: Literal["confirmed", "modified", "cancelled"]

    @field_validator("check_out")
    @classmethod
    def check_out_after_check_in(cls, value: str, info):
        check_in = info.data.get("check_in")
        if check_in and value <= check_in:
            raise ValueError("check_out must be later than check_in")
        return value


class ReservationEvent(BaseModel):
    """Envelope shared by all events published by this source."""

    model_config = ConfigDict(extra="forbid")

    event_id: UUID
    schema_version: Literal["1.0"]
    source: Literal["f2-channel-manager"]
    event_type: EventType
    occurred_at: datetime
    hotel_id: str = Field(pattern=r"^HP-[A-Z]{2}-\d{3}$")
    reservation_id: str = Field(min_length=1, max_length=80)
    data: ReservationPayload

    @field_validator("occurred_at")
    @classmethod
    def timestamp_must_be_timezone_aware(cls, value: datetime) -> datetime:
        if value.tzinfo is None or value.utcoffset() is None:
            raise ValueError("occurred_at must include a timezone")
        return value.astimezone(UTC)


def build_event(index: int, seed: int = 42) -> ReservationEvent:
    """Build a reproducible synthetic event for local testing."""
    if index < 0:
        raise ValueError("index must be non-negative")

    rng = random.Random(f"{seed}:{index}")
    hotel_id = HOTEL_IDS[index % len(HOTEL_IDS)]
    country = hotel_id.split("-")[1]
    event_type: EventType = (
        "reservation.created",
        "reservation.updated",
        "reservation.cancelled",
    )[index % 3]
    status = {
        "reservation.created": "confirmed",
        "reservation.updated": "modified",
        "reservation.cancelled": "cancelled",
    }[event_type]
    check_in = date(2026, 11, 1) + timedelta(days=index % 25)
    checkout = check_in + timedelta(days=rng.randint(1, 5))
    occurred_at = datetime(2026, 10, 1, tzinfo=UTC) + timedelta(minutes=index)

    if country == "CO":
        currency = "COP"
    elif country in {"PT", "ES"}:
        currency = "EUR"
    elif country == "BR":
        currency = "BRL"
    else:
        currency = "MXN"

    return ReservationEvent(
        event_id=uuid5(NAMESPACE_URL, f"buenavista-streaming:{seed}:{index}"),
        schema_version="1.0",
        source="f2-channel-manager",
        event_type=event_type,
        occurred_at=occurred_at,
        hotel_id=hotel_id,
        reservation_id=f"F2-RES-{seed:04d}-{index:08d}",
        data=ReservationPayload(
            check_in=check_in.isoformat(),
            check_out=checkout.isoformat(),
            room_type=("standard", "deluxe", "suite")[index % 3],
            guests=rng.randint(1, 4),
            total_amount=round(rng.uniform(80, 900), 2),
            currency=currency,
            status=status,
        ),
    )
