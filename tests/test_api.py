"""Unit tests for the FastAPI-to-Kafka boundary."""

from fastapi.testclient import TestClient

from src import api

client = TestClient(api.app)


def test_health_endpoint_does_not_claim_downstream_health():
    response = client.get("/health")

    assert response.status_code == 200
    assert response.json() == {"status": "ok"}


def test_create_reservation_publishes_validated_event(monkeypatch):
    published = {}

    def fake_publish(event):
        published["event"] = event
        return "reservation.events.v1_2"

    monkeypatch.setattr(api, "publish_to_kafka", fake_publish)
    response = client.post(
        "/reservations",
        json={
            "hotel_id": "HP-CO-001",
            "reservation_id": "F2-API-TEST-0001",
            "data": {
                "check_in": "2026-11-10",
                "check_out": "2026-11-12",
                "room_type": "DOUBLE",
                "guests": 2,
                "total_amount": 640000,
                "currency": "COP",
                "status": "confirmed",
                "channel_code": "expedia",
                "refundable": True,
            },
        },
    )

    assert response.status_code == 202
    assert response.json()["status"] == "accepted_by_kafka"
    assert response.json()["topic"] == "reservation.events.v1_2"
    assert published["event"].event_type == "reservation.created"
    assert published["event"].reservation_version == 1


def test_create_reservation_rejects_invalid_payload():
    response = client.post(
        "/reservations",
        json={
            "hotel_id": "invalid",
            "reservation_id": "F2-API-TEST-0002",
            "data": {},
        },
    )

    assert response.status_code == 422
