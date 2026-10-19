"""Contract tests kept independent of a running Kafka cluster or Neon database."""
import json
from pydantic import ValidationError
import pytest
from src.events import ReservationEvent, build_event

def test_v11_event_round_trip():
    event = build_event(index=0, seed=42)
    decoded = ReservationEvent.model_validate_json(event.model_dump_json())
    assert decoded.event_id == event.event_id
    assert decoded.reservation_id == event.reservation_id
    assert decoded.schema_version == "1.1"

def test_direct_guest_identifiers_are_not_in_f2_payload():
    event = build_event(index=0, seed=42)
    payload = json.loads(event.model_dump_json())
    assert {"guest_name", "email", "phone", "payment_card"}.isdisjoint(payload["data"])

def test_unreviewed_fields_are_rejected():
    event = build_event(index=0, seed=42)
    payload = json.loads(event.model_dump_json())
    payload["unreviewed_field"] = "not allowed"
    with pytest.raises(ValidationError):
        ReservationEvent.model_validate(payload)
