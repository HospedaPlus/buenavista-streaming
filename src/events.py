"""Define and generate the versioned, privacy-aware F2 reservation event contract.

All records created here are synthetic. This module does not connect to a real
channel manager or retrieve real guest or reservation information.

The generator simulates the full reservation lifecycle emitted by a channel
manager: creation, zero or more modifications, and one outcome (cancellation,
no-show, or check-in followed by check-out). Events are emitted in simulated
chronological order, except for a configurable share that arrives out of order
(for example, a modification before its creation), as required for F2.
"""

import heapq
import json
import random
from collections.abc import Iterator
from datetime import UTC, date, datetime, time, timedelta
from itertools import islice
from pathlib import Path
from typing import Literal
from uuid import NAMESPACE_URL, UUID, uuid5

from pydantic import BaseModel, ConfigDict, Field, field_validator

# Literal values restrict event types to the lifecycle operations F2 requires.
EventType = Literal[
    "reservation.created",
    "reservation.updated",
    "reservation.cancelled",
    "reservation.no_show",
    "reservation.checked_in",
    "reservation.checked_out",
]
EVENT_TYPES: tuple[str, ...] = EventType.__args__

ReservationStatus = Literal[
    "confirmed",
    "modified",
    "cancelled",
    "no_show",
    "checked_in",
    "checked_out",
]
STATUS_BY_EVENT_TYPE: dict[str, str] = {
    "reservation.created": "confirmed",
    "reservation.updated": "modified",
    "reservation.cancelled": "cancelled",
    "reservation.no_show": "no_show",
    "reservation.checked_in": "checked_in",
    "reservation.checked_out": "checked_out",
}

# Sales channels used by the commission (RN-02) and cancellation (RN-07) rules.
ChannelCode = Literal["booking_channel_a", "booking_channel_b", "expedia", "direct_ota"]

SCHEMA_VERSION = "1.2"

# Reuse the project's shared hotel identifier set to avoid generating unknown hotels.
CATALOG_PATH = Path(__file__).resolve().parents[1] / "catalog" / "catalog.json"
CATALOG = json.loads(CATALOG_PATH.read_text(encoding="utf-8"))
HOTELS = CATALOG["hotels"]
HOTEL_BY_CODE = {hotel["code"]: hotel for hotel in HOTELS}
HOTEL_IDS = tuple(HOTEL_BY_CODE)
ROOM_TYPES = {item["code"]: item for item in CATALOG["room_types"]}
# F2 owns OTA integrations. F3 direct/corporate and F4 agencies are not emitted by F2.
F2_CHANNELS = tuple(channel for channel in CATALOG["channels"] if channel["source"] == "F2")
F2_CHANNEL_CODES = tuple(channel["code"] for channel in F2_CHANNELS)

# Simulated clock origin. occurred_at values are simulated business time, which
# advances much faster than wall-clock time so a short run covers whole stays.
SIMULATION_START = datetime(2026, 10, 1, tzinfo=UTC)

# Default share of events delivered out of order.
DEFAULT_OUT_OF_ORDER_RATE = 0.05

# Mean simulated minutes between two new bookings across the whole chain.
MEAN_MINUTES_BETWEEN_BOOKINGS = 20

# Synthetic nightly rates by canonical room type and hotel currency; not production pricing.
NIGHTLY_BASE_RATE = {
    "COP": {"SINGLE": 240_000, "DOUBLE": 320_000, "TWIN": 330_000, "SUITE": 680_000},
    "EUR": {"SINGLE": 90, "DOUBLE": 120, "TWIN": 125, "SUITE": 260},
    "BRL": {"SINGLE": 380, "DOUBLE": 500, "TWIN": 520, "SUITE": 1_050},
    "MXN": {"SINGLE": 1_300, "DOUBLE": 1_700, "TWIN": 1_750, "SUITE": 3_600},
}
F2_CHANNEL_WEIGHTS = {
    "booking_channel_a": 0.30,
    "booking_channel_b": 0.25,
    "expedia": 0.30,
    "direct_ota": 0.15,
}
REFUNDABLE_SHARE = 0.70

# Outcome shares. The 37% cancellation rate comes from the business context.
CANCELLATION_RATE = 0.37
NO_SHOW_RATE = 0.05
# Share of cancellations made less than 24 hours before arrival.
LATE_CANCELLATION_SHARE = 0.30
# Number of modifications per reservation and its weight.
MODIFICATION_WEIGHTS = {0: 0.50, 1: 0.30, 2: 0.15, 3: 0.05}

CHECK_IN_TIME = time(15, 0)
CHECK_OUT_TIME = time(11, 0)


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
    room_type: Literal["SINGLE", "DOUBLE", "TWIN", "SUITE"]
    guests: int = Field(ge=1, le=20)
    total_amount: float = Field(ge=0)
    currency: str = Field(pattern=r"^[A-Z]{3}$")
    status: ReservationStatus
    channel_code: ChannelCode
    refundable: bool

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
    schema_version: Literal["1.2"]
    source: Literal["f2-channel-manager"]
    event_type: EventType
    occurred_at: datetime
    hotel_id: str = Field(pattern=r"^HP-[A-Z]{2}-\d{3}$")
    reservation_id: str = Field(min_length=1, max_length=80)
    # Position of this event in its reservation's lifecycle, starting at 1.
    # Consumers use it to order events that arrive out of order.
    reservation_version: int = Field(ge=1)
    data: ReservationPayload

    @field_validator("occurred_at")
    @classmethod
    def timestamp_must_be_timezone_aware(cls, value: datetime) -> datetime:
        """Require a timezone and normalize timestamps to UTC."""
        if value.tzinfo is None or value.utcoffset() is None:
            raise ValueError("occurred_at must include a timezone")
        return value.astimezone(UTC)


def _weighted_choice(rng: random.Random, weights: dict):
    """Pick one key of weights with probability proportional to its value."""
    return rng.choices(list(weights), weights=list(weights.values()))[0]


def _at(day: date, clock: time) -> datetime:
    """Combine a stay date and a local hotel time into a UTC timestamp."""
    return datetime.combine(day, clock, tzinfo=UTC)


def build_reservation_events(seed: int, number: int, booked_at: datetime) -> list[ReservationEvent]:
    """Build the complete, chronologically ordered lifecycle of one reservation.

    The same seed and reservation number always yield the same events.
    """
    rng = random.Random(f"{seed}:{number}")
    hotel = HOTELS[number % len(HOTELS)]
    hotel_id = hotel["code"]
    currency = hotel["currency"]
    reservation_id = f"F2-RES-{seed:04d}-{number:08d}"

    channel_code = _weighted_choice(rng, F2_CHANNEL_WEIGHTS)
    refundable = rng.random() < REFUNDABLE_SHARE

    # Most bookings are made a few days ahead; a few are made weeks ahead.
    lead_days = min(int(rng.expovariate(1 / 6)), 60)
    check_in = (booked_at + timedelta(days=lead_days)).date()
    if _at(check_in, CHECK_IN_TIME) <= booked_at + timedelta(hours=2):
        check_in += timedelta(days=1)
    arrival = _at(check_in, CHECK_IN_TIME)

    room_type = rng.choice(tuple(ROOM_TYPES))
    nights = rng.randint(1, 5)
    guests = rng.randint(1, ROOM_TYPES[room_type]["max_guests"])
    nightly = NIGHTLY_BASE_RATE[currency][room_type] * rng.uniform(0.8, 1.3)

    def snapshot(status: str) -> dict:
        return {
            "check_in": check_in.isoformat(),
            "check_out": (check_in + timedelta(days=nights)).isoformat(),
            "room_type": room_type,
            "guests": guests,
            "total_amount": round(nightly * nights, 2),
            "currency": currency,
            "status": status,
            "channel_code": channel_code,
            "refundable": refundable,
        }

    # Decide the outcome first so modifications happen before it.
    outcome = rng.random()
    if outcome < CANCELLATION_RATE:
        if rng.random() < LATE_CANCELLATION_SHARE:
            cancelled_at = arrival - timedelta(minutes=rng.randint(60, 23 * 60))
        else:
            latest = max(arrival - timedelta(hours=24), booked_at + timedelta(minutes=10))
            cancelled_at = booked_at + (latest - booked_at) * rng.uniform(0.2, 1.0)
        cancelled_at = max(cancelled_at, booked_at + timedelta(minutes=5))
        modifications_until = cancelled_at
    else:
        cancelled_at = None
        modifications_until = arrival - timedelta(hours=1)

    steps: list[tuple[str, datetime, dict]] = [
        ("reservation.created", booked_at, snapshot("confirmed"))
    ]

    # Multiple modifications: dates, room, guests and amount can change.
    modification_count = _weighted_choice(rng, MODIFICATION_WEIGHTS)
    window = max(modifications_until - booked_at, timedelta(minutes=2))
    offsets = sorted(rng.uniform(0.05, 0.95) for _ in range(modification_count))
    for offset in offsets:
        change = rng.choice(("nights", "room_type", "guests"))
        if change == "nights":
            nights = max(1, nights + rng.choice((-1, 1)))
        elif change == "room_type":
            room_type = rng.choice([r for r in ROOM_TYPES if r != room_type])
            nightly = NIGHTLY_BASE_RATE[currency][room_type] * rng.uniform(0.8, 1.3)
        else:
            guests = max(1, min(ROOM_TYPES[room_type]["max_guests"], guests + rng.choice((-1, 1))))
        guests = min(guests, ROOM_TYPES[room_type]["max_guests"])
        steps.append(("reservation.updated", booked_at + window * offset, snapshot("modified")))

    if cancelled_at is not None:
        steps.append(("reservation.cancelled", cancelled_at, snapshot("cancelled")))
    elif outcome < CANCELLATION_RATE + NO_SHOW_RATE:
        steps.append(("reservation.no_show", _at(check_in, time(23, 0)), snapshot("no_show")))
    else:
        checked_in_at = arrival + timedelta(minutes=rng.randint(0, 6 * 60))
        departure = _at(check_in + timedelta(days=nights), CHECK_OUT_TIME)
        checked_out_at = departure - timedelta(minutes=rng.randint(0, 3 * 60))
        steps.append(("reservation.checked_in", checked_in_at, snapshot("checked_in")))
        steps.append(("reservation.checked_out", checked_out_at, snapshot("checked_out")))

    events = []
    previous_at = None
    for version, (event_type, occurred_at, data) in enumerate(steps, start=1):
        # Keep every reservation's own timeline strictly increasing.
        if previous_at is not None and occurred_at <= previous_at:
            occurred_at = previous_at + timedelta(seconds=1)
        previous_at = occurred_at
        events.append(
            ReservationEvent(
                event_id=uuid5(
                    NAMESPACE_URL, f"buenavista-streaming:{seed}:{reservation_id}:{version}"
                ),
                schema_version=SCHEMA_VERSION,
                source="f2-channel-manager",
                event_type=event_type,
                occurred_at=occurred_at,
                hotel_id=hotel_id,
                reservation_id=reservation_id,
                reservation_version=version,
                data=ReservationPayload(**data),
            )
        )
    return events


def generate_events(
    seed: int = 42,
    out_of_order_rate: float = DEFAULT_OUT_OF_ORDER_RATE,
    start: datetime = SIMULATION_START,
) -> Iterator[ReservationEvent]:
    """Yield an endless, reproducible stream of F2 channel-manager events.

    Reservations from all hotels are interleaved in simulated chronological
    order. With probability out_of_order_rate an event is held back and
    delivered right after the next event of the same reservation, so
    consumers see, for example, a modification before its creation.
    """
    if not 0 <= out_of_order_rate <= 1:
        raise ValueError("out_of_order_rate must be between 0 and 1")

    rng = random.Random(f"{seed}:stream")
    pending: list[tuple[datetime, int, int, bool, ReservationEvent]] = []
    held: dict[str, ReservationEvent] = {}
    next_booking = start
    number = 0

    while True:
        # Bookings arrive in time order, so generate reservations lazily until
        # the earliest pending event is earlier than the next booking.
        while not pending or next_booking <= pending[0][0]:
            lifecycle = build_reservation_events(seed, number, next_booking)
            for event in lifecycle:
                is_last = event.reservation_version == len(lifecycle)
                entry = (event.occurred_at, number, event.reservation_version, is_last, event)
                heapq.heappush(pending, entry)
            number += 1
            next_booking += timedelta(minutes=rng.expovariate(1 / MEAN_MINUTES_BETWEEN_BOOKINGS))

        _, _, _, is_last, event = heapq.heappop(pending)
        earlier = held.pop(event.reservation_id, None)
        if earlier is None and not is_last and rng.random() < out_of_order_rate:
            held[event.reservation_id] = event
            continue

        yield event
        if earlier is not None:
            yield earlier


def build_event(index: int, seed: int = 42) -> ReservationEvent:
    """Return the event at a given position of the deterministic stream.

    A fixed seed and index always yield the same event, which makes debugging
    repeatable without using real booking data.
    """
    if index < 0:
        raise ValueError("index must be non-negative")
    return next(islice(generate_events(seed), index, None))
