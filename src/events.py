"""Define and generate the versioned, privacy-aware F2 reservation event contract.

All records created here are synthetic. This module does not connect to a real
channel manager or retrieve real guest or reservation information.
"""

import json
import random
from datetime import UTC, date, datetime, timedelta
from pathlib import Path
from typing import Literal
from uuid import NAMESPACE_URL, UUID, uuid5

from pydantic import BaseModel, ConfigDict, Field, field_validator

# Literal values restrict event types to the lifecycle operations supported today.
EventType = Literal[
    "reservation.created",
    "reservation.updated",
    "reservation.cancelled",
]

# Reuse the project's shared hotel identifier set to avoid generating unknown hotels.
CATALOG_PATH = Path(__file__).parent / "catalog" / "hotel_ids.json"
HOTEL_IDS = json.loads(CATALOG_PATH.read_text(encoding="utf-8"))


class ReservationPayload(BaseModel):
    """Validate reservation facts carried inside an event.

    Extra fields are rejected so direct identifiers or unreviewed fields are not
    silently added to the event payload.
    """

    model_config = ConfigDict(extra="forbid")

    # ISO date strings; the validator below also checks their ordering.
    check_in: str = Field(pattern=r"^\d{4}-\d{2}-\d{2}$")
    check_out: str = Field(pattern=r"^\d{4}-\d{2}-\d{2}$")

    # Bound text length and numeric ranges to reject clearly invalid sample data.
    room_type: str = Field(min_length=1, max_length=80)
    guests: int = Field(ge=1, le=20)
    total_amount: float = Field(ge=0)
    currency: str = Field(pattern=r"^[A-Z]{3}$")
    status: Literal["confirmed", "modified", "cancelled"]

    @field_validator("check_out")
    @classmethod
    def check_out_after_check_in(cls, value: str, info):
        """Reject a checkout date that is not later than check-in."""
        check_in = info.data.get("check_in")
        if check_in and value <= check_in:
            raise ValueError("check_out must be later than check_in")
        return value


class ReservationEvent(BaseModel):
    """Validate the common envelope used by every published reservation event."""

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
        """Require a timezone and normalize timestamps to UTC."""
        if value.tzinfo is None or value.utcoffset() is None:
            raise ValueError("occurred_at must include a timezone")
        return value.astimezone(UTC)


def build_event(index: int, seed: int = 42) -> ReservationEvent:
    """Create one deterministic synthetic event for local development and tests.

    A fixed seed and index always yield the same event ID and values, which makes
    debugging repeatable without using real booking data.
    """
    if index < 0:
        raise ValueError("index must be non-negative")

    # Give each event its own seeded generator so calls are independent of order.
    rng = random.Random(f"{seed}:{index}")
    hotel_id = HOTEL_IDS[index % len(HOTEL_IDS)]
    country = hotel_id.split("-")[1]

    # Cycle through event types so a small batch exercises all lifecycle states.
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

    # Generate plausible but synthetic dates and a stable event timestamp.
    check_in = date(2026, 11, 1) + timedelta(days=index % 25)
    checkout = check_in + timedelta(days=rng.randint(1, 5))
    occurred_at = datetime(2026, 10, 1, tzinfo=UTC) + timedelta(minutes=index)

    # Assign a currency based on the country code in the shared hotel identifier.
    if country == "CO":
        currency = "COP"
    elif country in {"PT", "ES"}:
        currency = "EUR"
    elif country == "BR":
        currency = "BRL"
    else:
        currency = "MXN"

    # Pydantic validates the finished object before the producer serializes it.
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
