# Reservation event contract

**Status:** version 1.0 development contract. Confirm with the data-platform owner before production use.

## Envelope

| Field | Type | Rule |
|---|---|---|
| `event_id` | UUID | Stable for a given synthetic seed and event index |
| `schema_version` | string | `1.0` |
| `source` | string | `f2-channel-manager` |
| `event_type` | enum | `reservation.created`, `reservation.updated`, `reservation.cancelled` |
| `occurred_at` | timestamp | Timezone-aware ISO 8601, normalized to UTC |
| `hotel_id` | string | ID from the shared catalog |
| `reservation_id` | string | Synthetic reservation identifier |
| `data` | object | Minimal reservation facts |

## RabbitMQ delivery

Messages are published to a durable queue configured by `RABBITMQ_QUEUE`. Messages use persistent delivery mode, JSON content type, and publisher confirms. The event ID is copied into the AMQP message ID property.

Publisher confirms do not mean a consumer has processed the event. A consumer must acknowledge only after its processing step succeeds, use idempotent writes keyed by `event_id`, and define retry/dead-letter behavior.

## Data quality and privacy

The source is synthetic. Do not include guest names, emails, phone numbers, payment card data, or other direct identifiers. Consumers should validate the schema version and retain both event and processing timestamps.

## Example

```json
{
  "event_id": "2f1c3f35-bbb3-5b0a-a0f8-ccfcf4dbfd4c",
  "schema_version": "1.0",
  "source": "f2-channel-manager",
  "event_type": "reservation.created",
  "occurred_at": "2026-10-01T00:00:00Z",
  "hotel_id": "HP-CO-001",
  "reservation_id": "F2-RES-0042-00000000",
  "data": {
    "check_in": "2026-11-01",
    "check_out": "2026-11-03",
    "room_type": "standard",
    "guests": 2,
    "total_amount": 245.7,
    "currency": "COP",
    "status": "confirmed"
  }
}
```
