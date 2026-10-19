# Reservation event contract

**Status:** F2 synthetic event contract v1.1. The envelope and lifecycle remain unchanged by the Kafka transport migration.

## Source alignment

F2 reads the pinned shared hotel ID list at `src/catalog/hotel_ids.json`; hotel IDs must match the canonical catalog in [HospedaPlus/db_simulated](https://github.com/HospedaPlus/db_simulated/tree/main/catalog). Do not edit the local copy in isolation. Melissa's F3 schema remains the source for F3 own-booking-engine reservations. F2 events are stored in the separate `streaming` schema and must not be inserted into F3's `reservations` table.

Events are synthetic and contain no guest identifiers or payment data. The event payload is a channel-manager projection, not an F3 reservation row.

## Envelope

| Field | Type | Rule |
|---|---|---|
| `event_id` | UUID | Stable for a given synthetic seed and event index |
| `schema_version` | string | `1.1` |
| `source` | string | `f2-channel-manager` |
| `event_type` | enum | created, updated, cancelled, no_show, checked_in, checked_out |
| `occurred_at` | timestamp | Timezone-aware ISO 8601, normalized to UTC |
| `hotel_id` | string | ID from the shared catalog |
| `reservation_id` | string | Synthetic F2 reservation identifier |
| `reservation_version` | integer ≥ 1 | Per-reservation lifecycle version |
| `data` | object | Minimal reservation facts |

Payload fields: `check_in`, `check_out`, `room_type`, `guests`, `total_amount`, `currency`, `status`, `channel`, `refundable`. Status follows event type; channel follows the existing F2 enum and RN-02 intent. Do not add direct identifiers.

## Kafka transport

- Topic: `buenavista.reservations.v1`; DLQ: `buenavista.reservations.dlq.v1`.
- The producer key is `reservation_id`, so Kafka preserves order for a reservation within its partition. The simulator may intentionally generate out-of-order lifecycle versions; consumers reconcile by `reservation_version`.
- Producer uses `acks=all` and idempotence. These protect broker publication; they do not make Neon and S3 writes atomic.
- Consumer commits offsets only after Bronze and Neon writes complete. Both sinks use `event_id` for replay-safe writes; the current-state projection accepts only higher reservation versions.
- Invalid JSON/schema records go to the DLQ before their source offset is committed. Sink failures leave the offset uncommitted for replay.

## Data destinations

Raw, unmodified event JSON lands in S3 Bronze at `bronze/source=f2_channel_manager/event_date=YYYY-MM-DD/event_type=.../event_id=....json` (or the equivalent local directory for development). Neon stores the event ledger and a current-state projection under the additive `streaming` schema. This does not modify the F3-owned tables.

The SQL in `sql/streaming_schema.sql` is a proposed additive migration and must be reviewed/applied by the database owner. Do not point F3 seed, reset, or database tests at the shared Neon production database.

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
