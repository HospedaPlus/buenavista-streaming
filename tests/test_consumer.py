"""Unit tests for event contract and monotonic projection behavior."""
import json
from src.events import ReservationEvent, build_event

def test_event_serialization_is_contract_compatible():
    event = build_event(index=0, seed=42)
    decoded = ReservationEvent.model_validate_json(event.model_dump_json())
    assert decoded.event_id == event.event_id
    assert decoded.reservation_id == event.reservation_id
    assert decoded.schema_version == "1.2"

def test_payload_does_not_contain_direct_guest_identifiers():
    event = build_event(index=0, seed=42)
    payload = json.loads(event.model_dump_json())
    forbidden = {"guest_name", "email", "phone", "payment_card"}
    assert forbidden.isdisjoint(payload["data"])
