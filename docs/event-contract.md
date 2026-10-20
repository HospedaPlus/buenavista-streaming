# Reservation event contract

**Version:** 1.2  
**Status:** development contract. Confirm with the data-platform owner before production use.  
**Canonical reference data:** pinned copy of `HospedaPlus/db_simulated/catalog/catalog.json`, version `1.1.0`.

## Envelope

| Field | Type | Rule |
|---|---|---|
| `event_id` | UUID | Stable for the same synthetic seed, reservation ID and lifecycle version |
| `schema_version` | string | `1.2` |
| `source` | string | `f2-channel-manager` |
| `event_type` | enum | `reservation.created`, `reservation.updated`, `reservation.cancelled`, `reservation.no_show`, `reservation.checked_in`, `reservation.checked_out` |
| `occurred_at` | timestamp | Timezone-aware ISO 8601, normalized to UTC; simulated business time |
| `hotel_id` | string | Exact `hotels[].code` from the shared catalog |
| `reservation_id` | string | Stable synthetic F2 identifier; not the F3 integer primary key |
| `reservation_version` | integer ≥ 1 | Lifecycle order for the reservation; reconcile out-of-order events by this value |
| `data` | object | Minimal reservation facts |

## Payload (`data`)

| Field | Type | Rule |
|---|---|---|
| `check_in`, `check_out` | date | ISO date; checkout later than check-in |
| `room_type` | string | Canonical catalog code: `SINGLE`, `DOUBLE`, `TWIN`, `SUITE` |
| `guests` | integer | At least 1 and no greater than the canonical room type's `max_guests` |
| `total_amount` | number | Gross amount for the whole stay, in `currency` |
| `currency` | string | Must match the canonical currency for the selected hotel |
| `status` | enum | `confirmed`, `modified`, `cancelled`, `no_show`, `checked_in`, `checked_out`; consistent with `event_type` |
| `channel_code` | string | Exact canonical channel code whose catalog `source` is `F2` |
| `refundable` | boolean | Synthetic cancellation-policy input |

F2 channel codes are read from the canonical catalog where `source = "F2"`. The simulator must not generate F3 direct/corporate or F4 agency channels. Channel code, not merely channel category, is retained because commission rules depend on the canonical channel.

Room types, hotel codes and currencies are read from the pinned shared catalog rather than maintained as independent lists. The generated prices are synthetic test values, not production pricing or a replacement for the shared pricing rules.

## Ordering and idempotency

A configurable share of events may arrive out of order. Consumers must retain the original event and reconcile operational state using `reservation_id` and `reservation_version`. Use `event_id` for event deduplication. Do not assume an update always arrives after creation.

## Database projection notes

The event `reservation_id` is not the F3 `reservations.id` integer key. The Neon sink must define an explicit stable mapping to `reservations.code`, resolve canonical hotel/room/channel foreign keys, and define how guest identity is handled. F3 operational status constraints do not include `modified`, `checked_in`, or `checked_out`; those lifecycle values must remain in the event ledger and must not be silently translated into a different operational meaning.

The event ledger should preserve the original validated payload and processing metadata. Operational writes and ledger writes must be atomic. Commit Kafka offsets only after the database transaction succeeds.

## Privacy

All generated records are synthetic. Do not include guest names, emails, phone numbers, payment-card data, or other direct identifiers.

## Example

```json
{
  "event_id": "8d95f529-7e3a-52c3-8c78-a33d03371e88",
  "schema_version": "1.2",
  "source": "f2-channel-manager",
  "event_type": "reservation.created",
  "occurred_at": "2026-10-01T00:00:00Z",
  "hotel_id": "HP-PT-001",
  "reservation_id": "F2-RES-0042-00000000",
  "reservation_version": 1,
  "data": {
    "check_in": "2026-10-12",
    "check_out": "2026-10-16",
    "room_type": "SUITE",
    "guests": 4,
    "total_amount": 895.64,
    "currency": "EUR",
    "status": "confirmed",
    "channel_code": "expedia",
    "refundable": true
  }
}
```
