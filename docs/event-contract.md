# Reservation event contract

**Status:** version 1.0 development contract. Confirm with the data-platform owner before production use.

## Envelope

| Field | Type | Rule |
|---|---|---|
| `event_id` | UUID | Stable for a given synthetic seed and event index |
| `schema_version` | string | `1.0` |
| `source` | string | `f2-channel-manager` |
| `event_type` | enum | `reservation.created`, `reservation.updated`, `reservation.cancelled` |
| `occurred_at` | timestamp | Timezone-aware ISO 8601; normalized to UTC |
| `hotel_id` | string | Shared catalog style, e.g. `HP-CO-001` |
| `reservation_id` | string | Synthetic source identifier |
| `data` | object | Minimal reservation fields |

## Payload

- `check_in`, `check_out`: ISO dates; checkout must be after check-in.
- `room_type`: non-empty label.
- `guests`: integer from 1 to 20.
- `total_amount`: non-negative numeric amount.
- `currency`: three-letter uppercase currency code.
- `status`: `confirmed`, `modified`, or `cancelled`.

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

## Data quality and privacy

The source is synthetic. Do not include guest names, emails, phone numbers, payment card data, or other direct identifiers in the event stream. Downstream consumers should validate schema version, deduplicate on `event_id`, retain source/event timestamps, and define retry and dead-letter behavior before production rollout.

## Delivery semantics

Kinesis `PutRecord` acknowledges an individual publish and returns a shard ID and sequence number. This scaffold does not yet implement a consumer, checkpointing, replay orchestration, or end-to-end sink verification. Do not claim exactly-once delivery; downstream processing must be idempotent.
