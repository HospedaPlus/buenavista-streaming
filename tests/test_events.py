import pytest
from pydantic import ValidationError

from src.events import HOTEL_IDS, ReservationEvent, ReservationPayload, build_event


def test_event_is_valid_and_uses_utc():
    event = build_event(0)
    assert isinstance(event, ReservationEvent)
    assert event.occurred_at.tzinfo is not None
    assert event.source == "f2-channel-manager"
    assert event.schema_version == "1.0"


def test_same_seed_and_index_produce_same_event():
    assert build_event(7, seed=99).model_dump_json() == build_event(7, seed=99).model_dump_json()


def test_event_uses_shared_hotel_id_set():
    assert len(HOTEL_IDS) == 28
    assert build_event(0).hotel_id in HOTEL_IDS
    assert build_event(27).hotel_id in HOTEL_IDS


def test_event_payload_has_no_direct_guest_identifiers():
    payload = build_event(3).model_dump()["data"]
    assert "guest_name" not in payload
    assert "email" not in payload
    assert "phone" not in payload


def test_checkout_must_be_after_checkin():
    with pytest.raises(ValidationError):
        ReservationPayload(
            check_in="2026-11-02",
            check_out="2026-11-01",
            room_type="standard",
            guests=2,
            total_amount=100,
            currency="COP",
            status="confirmed",
        )
