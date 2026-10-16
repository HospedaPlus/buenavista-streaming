# Reservation event contract

**Status:** version 1.1 development contract. Confirm with the data-platform owner before production use.

## Envelope

| Field | Type | Rule |
|---|---|---|
| `event_id` | UUID | Stable for a given synthetic seed and event index |
| `schema_version` | string | `1.1` |
| `source` | string | `f2-channel-manager` |
| `event_type` | enum | `reservation.created`, `reservation.updated`, `reservation.cancelled`, `reservation.no_show`, `reservation.checked_in`, `reservation.checked_out` |
| `occurred_at` | timestamp | Timezone-aware ISO 8601, normalized to UTC. Simulated business time (it advances faster than wall-clock time) |
| `hotel_id` | string | ID from the shared catalog |
| `reservation_id` | string | Synthetic reservation identifier |
| `reservation_version` | integer ≥ 1 | Position of the event in its reservation's lifecycle; use it to order out-of-order events |
| `data` | object | Minimal reservation facts (below) |

## Payload (`data`)

| Field | Type | Rule |
|---|---|---|
| `check_in`, `check_out` | date | ISO date; `check_out` later than `check_in` |
| `room_type` | string | `standard`, `deluxe` or `suite` |
| `guests` | integer | 1 to 20 |
| `total_amount` | number | Gross amount in `currency` for the whole stay |
| `currency` | string | ISO 4217 code by hotel country |
| `status` | enum | `confirmed`, `modified`, `cancelled`, `no_show`, `checked_in`, `checked_out` (follows `event_type`) |
| `channel` | enum | `ota`, `direct`, `agency`, `corporate` (commission rule RN-02) |
| `refundable` | boolean | Cancellation policy input (RN-07) |

Changes from 1.0: three new event types, `reservation_version`, `channel` and `refundable`.

## Ordering

Each reservation's events are emitted in order of `occurred_at`, except a configurable share (`--out-of-order-rate`, default 5%) that is delivered right after the next event of the same reservation. A consumer must therefore accept an update, cancellation or outcome before the creation and reconcile by `reservation_id` and `reservation_version` (acceptance criterion CA-09).

## RabbitMQ delivery

Messages are published to the durable topic exchange `RABBITMQ_EXCHANGE` with the event type as routing key (for example `reservation.cancelled`). The durable queue `RABBITMQ_QUEUE` is bound with `reservation.#`, and other consumers can bind their own queues to a subset of types. Messages use persistent delivery mode, JSON content type, and publisher confirms. The event ID is copied into the AMQP message ID property.

Publisher confirms do not mean a consumer has processed the event. A consumer must acknowledge only after its processing step succeeds, use idempotent writes keyed by `event_id`, and define retry/dead-letter behavior.

## Data quality and privacy

The source is synthetic. Do not include guest names, emails, phone numbers, payment card data, or other direct identifiers. Consumers should validate the schema version and retain both event and processing timestamps.

## Example

```json
{
  "event_id": "8d95f529-7e3a-52c3-8c78-a33d03371e88",
  "schema_version": "1.1",
  "source": "f2-channel-manager",
  "event_type": "reservation.created",
  "occurred_at": "2026-10-01T00:00:00Z",
  "hotel_id": "HP-PT-001",
  "reservation_id": "F2-RES-0042-00000000",
  "reservation_version": 1,
  "data": {
    "check_in": "2026-10-12",
    "check_out": "2026-10-16",
    "room_type": "suite",
    "guests": 4,
    "total_amount": 895.64,
    "currency": "EUR",
    "status": "confirmed",
    "channel": "ota",
    "refundable": true
  }
}
```
