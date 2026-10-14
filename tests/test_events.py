from pydantic import ValidationError
import pytest

from src.events import ReservationEvent, build_event


def test_event_is_valid_and_uses_utc():
    event = build_event(0)
    assert isinstance(event, ReservationEvent)
    assert event.occurred_at.tzinfo is not None
    assert event.source == "f2-channel-manager"
    assert event.schema_version == "1.0"


def test_same_seed_and_index_produce_same_event():
    assert build_event(7, seed=99).model_dump_json() == build_event(7, seed=99).model_dump_json()


def test_event_payload_has_no_direct_guest_identifiers():
    event = build_event(3)
    payload = event.model_dump()
    assert "guest_name" not in payload["data"]
    assert "email" not in payload["data"]
    assert "phone" not in payload["data"]


def test_checkout_must_be_after_checkin():
    with pytest.raises(ValidationError):
        build_event(0).data.model_copy(update={"check_out": "2026-11-01", "check_in": "2026-11-02"}).model_dump()
