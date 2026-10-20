"""Unit tests for the synthetic reservation event contract."""

from collections import defaultdict
from datetime import UTC, datetime, timedelta
from itertools import islice

import pytest
from pydantic import ValidationError

from src.events import (
    CATALOG,
    EVENT_TYPES,
    F2_CHANNEL_CODES,
    HOTEL_IDS,
    ROOM_TYPES,
    STATUS_BY_EVENT_TYPE,
    ReservationEvent,
    ReservationPayload,
    build_event,
    generate_events,
)


def test_event_is_valid_and_uses_utc():
    """Generated events should have the expected envelope and timezone-aware timestamp."""
    event = build_event(0)
    assert isinstance(event, ReservationEvent)
    assert event.occurred_at.tzinfo is not None
    assert event.source == "f2-channel-manager"
    assert event.schema_version == "1.2"


def test_same_seed_and_index_produce_same_event():
    """A fixed seed/index pair should make test data reproducible."""
    first = build_event(7, seed=99).model_dump_json()
    second = build_event(7, seed=99).model_dump_json()
    assert first == second


def test_event_uses_shared_hotel_id_set():
    """The generator should choose hotel identifiers from the shared 28-hotel set."""
    assert len(HOTEL_IDS) == 28
    assert {event.hotel_id for event in islice(generate_events(), 500)} <= set(HOTEL_IDS)


def test_event_payload_has_no_direct_guest_identifiers():
    """Keep direct guest identifiers out of the synthetic reservation payload."""
    payload = build_event(3).model_dump()["data"]
    assert "guest_name" not in payload
    assert "email" not in payload
    assert "phone" not in payload


def test_checkout_must_be_after_checkin():
    """Invalid reservation date ordering should be rejected by Pydantic."""
    with pytest.raises(ValidationError):
        ReservationPayload(
            check_in="2026-11-02",
            check_out="2026-11-01",
            room_type="SINGLE",
            guests=2,
            total_amount=100,
            currency="COP",
            status="confirmed",
            channel_code="expedia",
            refundable=True,
        )


def take(count, **kwargs):
    """Return the first count events of the deterministic stream."""
    return list(islice(generate_events(**kwargs), count))


def test_payload_rejects_guests_over_room_capacity():
    """Payload validation must enforce canonical room capacities."""
    with pytest.raises(ValidationError, match="supports at most"):
        ReservationPayload(
            check_in="2026-11-02",
            check_out="2026-11-04",
            room_type="SINGLE",
            guests=2,
            total_amount=100,
            currency="COP",
            status="confirmed",
            channel_code="expedia",
            refundable=True,
        )


def test_stream_covers_all_f2_event_types():
    """F2 requires created, modified, cancelled, no-show, check-in and check-out events."""
    seen = {event.event_type for event in take(3000)}
    assert seen == set(EVENT_TYPES)
    assert len(EVENT_TYPES) == 6


def test_status_matches_event_type():
    """Each event's payload status reflects its lifecycle step."""
    for event in take(500):
        assert event.data.status == STATUS_BY_EVENT_TYPE[event.event_type]


def test_stream_is_reproducible_with_same_seed():
    """The same seed must produce exactly the same stream."""
    first = [e.model_dump_json() for e in take(300, seed=7)]
    second = [e.model_dump_json() for e in take(300, seed=7)]
    assert first == second
    assert first != [e.model_dump_json() for e in take(300, seed=8)]


def test_event_ids_are_unique():
    """Event IDs are the idempotency key, so they must not repeat."""
    events = take(2000)
    assert len({e.event_id for e in events}) == len(events)


def test_lifecycles_are_consistent():
    """Each reservation starts with creation and ends in at most one outcome."""
    by_reservation = defaultdict(list)
    for event in take(3000, out_of_order_rate=0):
        by_reservation[event.reservation_id].append(event)

    for events in by_reservation.values():
        types = [e.event_type for e in events]
        assert types[0] == "reservation.created"
        assert [e.reservation_version for e in events] == list(range(1, len(events) + 1))
        times = [e.occurred_at for e in events]
        assert times == sorted(times)
        outcomes = {"reservation.cancelled", "reservation.no_show", "reservation.checked_in"}
        assert len(outcomes.intersection(types)) <= 1
        if "reservation.checked_out" in types:
            assert (
                types.index("reservation.checked_in") == types.index("reservation.checked_out") - 1
            )


def test_without_out_of_order_events_follow_simulated_time():
    """With out-of-order disabled the stream is chronological."""
    times = [e.occurred_at for e in take(2000, out_of_order_rate=0)]
    assert times == sorted(times)


def test_out_of_order_injection_delivers_updates_before_creation():
    """Some modifications must arrive before their creation event (CA-09)."""
    events = take(3000, out_of_order_rate=0.2)
    created_seen = set()
    update_before_create = 0
    for event in events:
        if event.event_type == "reservation.created":
            created_seen.add(event.reservation_id)
        elif event.reservation_id not in created_seen:
            update_before_create += 1
    assert update_before_create > 0


def test_stream_contains_multiple_modifications_and_late_cancellations():
    """F2 requires reservations with several modifications and late cancellations."""
    events = take(5000, out_of_order_rate=0)
    updates = defaultdict(int)
    late_cancellations = 0
    for event in events:
        if event.event_type == "reservation.updated":
            updates[event.reservation_id] += 1
        if event.event_type == "reservation.cancelled":
            arrival = datetime.fromisoformat(event.data.check_in).replace(hour=15, tzinfo=UTC)
            if arrival - event.occurred_at < timedelta(hours=24):
                late_cancellations += 1
    assert max(updates.values()) >= 2
    assert late_cancellations > 0


def test_cancellation_rate_is_close_to_business_context():
    """About 37% of reservations are cancelled, as stated in the business context."""
    outcomes = [e.event_type for e in take(20000, out_of_order_rate=0)]
    created = outcomes.count("reservation.created")
    cancelled = outcomes.count("reservation.cancelled")
    assert 0.30 < cancelled / created < 0.44


def test_invalid_out_of_order_rate_is_rejected():
    with pytest.raises(ValueError):
        next(generate_events(out_of_order_rate=1.5))


def test_event_catalog_codes_match_shared_catalog():
    """Generated identifiers and dimensions must use canonical catalog values."""
    assert len(HOTEL_IDS) == 28
    assert set(HOTEL_IDS) == {hotel["code"] for hotel in CATALOG["hotels"]}
    assert set(ROOM_TYPES) == {room["code"] for room in CATALOG["room_types"]}
    assert set(F2_CHANNEL_CODES) == {
        channel["code"] for channel in CATALOG["channels"] if channel["source"] == "F2"
    }


def test_generated_dimensions_are_canonical_and_capacity_valid():
    """Generated room types, currencies, channels and guest counts must match catalog."""
    hotels = {hotel["code"]: hotel for hotel in CATALOG["hotels"]}
    channels = {channel["code"]: channel for channel in CATALOG["channels"]}
    for event in take(500):
        payload = event.data
        assert payload.room_type in ROOM_TYPES
        assert payload.channel_code in F2_CHANNEL_CODES
        assert payload.currency == hotels[event.hotel_id]["currency"]
        assert payload.guests <= ROOM_TYPES[payload.room_type]["max_guests"]
        assert channels[payload.channel_code]["source"] == "F2"
