"""Unit tests for the synthetic reservation event contract."""

from collections import defaultdict
from datetime import datetime, timedelta
from itertools import islice

import pytest
from pydantic import ValidationError

from src.events import (
    CATALOG,
    CHECK_IN_TIME,
    EVENT_TYPES,
    F2_CHANNEL_CODES,
    HOTEL_BY_CODE,
    HOTEL_IDS,
    HOTEL_TZ,
    ROOM_TYPES,
    SIMULATION_START,
    STATUS_BY_EVENT_TYPE,
    DemandSurge,
    ReservationEvent,
    ReservationPayload,
    build_event,
    build_reservation_events,
    generate_events,
    loyalty_tier,
    mean_minutes_between_bookings,
)


def test_event_is_valid_and_uses_utc():
    """Generated events should have the expected envelope and timezone-aware timestamp."""
    event = build_event(0)
    assert isinstance(event, ReservationEvent)
    assert event.occurred_at.tzinfo is not None
    assert event.source == "f2-channel-manager"
    assert event.schema_version == "1.3"


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
            guest_ref="G-0000001",
            loyalty_tier="none",
            nightly_rate=50,
            total_amount=100,
            currency="COP",
            status="confirmed",
            channel_code="expedia",
            refundable=True,
        )


def local_arrival(event):
    """Check-in time (15:00) in the hotel's own timezone."""
    day = datetime.fromisoformat(event.data.check_in)
    return datetime.combine(day.date(), CHECK_IN_TIME, tzinfo=HOTEL_TZ[event.hotel_id])


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
            guest_ref="G-0000001",
            loyalty_tier="none",
            nightly_rate=50,
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
            arrival = local_arrival(event)
            if arrival - event.occurred_at < timedelta(hours=24):
                late_cancellations += 1
    assert max(updates.values()) >= 2
    assert late_cancellations > 0


def test_cancellation_rate_is_close_to_business_context():
    """About 37% of reservations are cancelled, as stated in the business context."""
    # Count whole lifecycles: a truncated stream would miss cancellations that
    # happen days after the booking.
    lifecycles = [build_reservation_events(42, number, SIMULATION_START) for number in range(5000)]
    cancelled = sum(
        any(e.event_type == "reservation.cancelled" for e in events) for events in lifecycles
    )
    assert 0.33 < cancelled / len(lifecycles) < 0.41


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


def test_check_in_uses_the_hotel_local_timezone():
    """A check-in is 15:00 local time, so in UTC it depends on the hotel's timezone."""
    for event in take(3000, out_of_order_rate=0):
        if event.event_type == "reservation.checked_in":
            local = event.occurred_at.astimezone(HOTEL_TZ[event.hotel_id])
            # Check-in happens between 15:00 and 21:00 local time.
            assert 15 <= local.hour <= 21
            assert local.date().isoformat() == event.data.check_in


def test_payload_carries_guest_reference_tier_and_nightly_rate():
    """RN-08 and RN-09 need a guest reference and tier; RN-07 needs the nightly rate."""
    for event in take(500):
        data = event.data
        assert data.guest_ref.startswith("G-")
        assert data.loyalty_tier in {"none", "silver", "gold"}
        nights = (
            datetime.fromisoformat(data.check_out) - datetime.fromisoformat(data.check_in)
        ).days
        assert data.total_amount == pytest.approx(data.nightly_rate * nights, abs=0.05)


def test_loyalty_tier_is_stable_per_guest():
    """The same guest always has the same tier, in every event and every run."""
    by_guest = {}
    for event in take(5000):
        tier = by_guest.setdefault(event.data.guest_ref, event.data.loyalty_tier)
        assert tier == event.data.loyalty_tier
    tiers = [loyalty_tier(n) for n in range(1, 20001)]
    assert 0.03 < tiers.count("gold") / len(tiers) < 0.07


def test_demand_is_calibrated_to_the_business_occupancy():
    """With 4,000 rooms, 68% occupancy and 50% F2 share, F2 creates ~700 bookings a day."""
    gap = mean_minutes_between_bookings()
    assert 1.5 < gap < 2.5
    events = take(20000, out_of_order_rate=0)
    created = [e for e in events if e.event_type == "reservation.created"]
    days = (created[-1].occurred_at - created[0].occurred_at).total_seconds() / 86400
    assert 550 < len(created) / days < 900


def test_bigger_hotels_receive_more_bookings():
    """Hotels are chosen in proportion to their rooms, not round-robin."""
    created = [e for e in take(30000, out_of_order_rate=0) if e.event_type == "reservation.created"]
    counts = defaultdict(int)
    for event in created:
        counts[event.hotel_id] += 1
    biggest = max(HOTEL_IDS, key=lambda code: HOTEL_BY_CODE[code]["rooms"])
    smallest = min(HOTEL_IDS, key=lambda code: HOTEL_BY_CODE[code]["rooms"])
    assert counts[biggest] > counts[smallest]


def test_demand_surge_concentrates_bookings_in_the_event_window():
    """A surge (e.g. a high-category event) adds bookings for that hotel and those dates."""
    surge = DemandSurge.parse("HP-PT-003:2026-10-20:2026-10-26:3.0")

    def bookings_in_window(**kwargs):
        return sum(
            1
            for e in take(20000, out_of_order_rate=0, **kwargs)
            if e.event_type == "reservation.created"
            and e.hotel_id == surge.hotel_id
            and surge.start.isoformat() <= e.data.check_in <= surge.end.isoformat()
        )

    assert bookings_in_window(surges=[surge]) > 1.5 * bookings_in_window()


def test_invalid_surge_is_rejected():
    with pytest.raises(ValueError):
        DemandSurge.parse("HP-XX-999:2026-10-20:2026-10-26:2.0")
    with pytest.raises(ValueError):
        DemandSurge.parse("HP-PT-001:2026-10-26:2026-10-20:2.0")
