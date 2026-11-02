# Neon sink design and integration gates

## Agreed model

The target model is both:
1. the existing F3 operational `reservations` table, for its supported current-state fields; and
2. an append-only `reservation_event_ledger`, retaining validated event payloads and Kafka provenance.

The additive DDL is in `migrations/001_create_reservation_event_ledger.sql`. It has not been applied to Neon.

## Implemented: applying events to F3 (`operation`)

`src/operation_sink.py` applies each newly inserted ledger event to F3 in the
same transaction, through the F3 functions (repo `db_simulated`):

| Event | F3 call | Outcomes |
|---|---|---|
| `reservation.created` | `operation.reservar()` | confirmed, rejected, already_exists |
| `reservation.updated` | `operation.modificar()` | modified, rejected, stale, not_modifiable, pending |
| `reservation.cancelled` | `operation.cancelar()` | cancelled, not_modifiable, pending |
| `reservation.no_show` | `operation.liberar(code, 'no_show')` | no_show, not_modifiable, pending |
| `checked_in` / `checked_out` | none | ignored |

- F3 code is `F2-<reservation_id>`; the guest comes from `guest_ref`, the first night from `nightly_rate`.
- A redelivered event is not inserted in the ledger again, so it is never applied twice.
- Out of order (CA-09): an event for a booking F3 doesn't have yet is `pending`; when the creation arrives, later ledger events for that reservation are replayed in version order.
- Data F3 rejects (unknown guest, check-in before booking date, missing rate) is `skipped` inside a savepoint: the ledger row still commits and the consumer keeps going.
- `NEON_APPLY_OPERATION=false` turns it off (ledger only). Requires F3 `seed init` on the same database first, so `operation.modificar()` exists.

The gates below are the original analysis; contract 1.3 and the F3 functions resolved them.

## Source-of-truth catalog

The F2 generator uses the pinned canonical catalog at `catalog/catalog.json`, currently version `1.1.0`. See `catalog/README.md`. Event schema version is `1.3`.

## Required transactional processing

For each Kafka message:
1. Validate the event contract and capture topic, partition, and offset.
2. Begin a PostgreSQL transaction.
3. Insert the ledger row using `event_id` as the idempotency key.
4. If the same event is already recorded, do not apply its operational projection again.
5. Resolve the reservation's canonical hotel, room-type, and channel foreign keys.
6. Apply operational state only when the event version is newer than the applied version.
7. Commit the PostgreSQL transaction.
8. Commit the Kafka offset only after the database transaction succeeds.

A failure before database commit must roll back both ledger and operational changes. Do not commit Kafka offsets on database failure.

## Mapping gates before implementing operational writes

Contract 1.3 adds a synthetic `guest_ref` (`G-NNNNNNN`) and `loyalty_tier`, which follows the first option below; the F3 seed must provision guests with the same codes and tiers. Contract 1.3 also adds `nightly_rate`, which gives the first-night amount. Before 1.3 the F2 contract contained no guest identifiers, while F3 `reservations.guest_id` is mandatory and references `guests.id`. The team must approve one of these approaches before the sink writes operational reservations:
- add a stable synthetic guest reference to the event contract and provision that guest through an approved synthetic-data process; or
- retain events in the ledger and defer operational projection until an upstream guest identity mapping is available.

Do not generate arbitrary guest rows or names inside the sink.

F3 accepts only `confirmed`, `cancelled`, and `no_show` as operational reservation statuses. Events with `modified`, `checked_in`, and `checked_out` must remain faithfully represented in the ledger. The data owner must approve whether these lifecycle events update only dates/amounts on an already supported status, or whether F3's operational schema should be changed in a separate reviewed migration. Do not silently translate lifecycle states.

The F2 `reservation_id` is a string, while F3 `reservations.id` is an integer identity. Use the stable F2 reservation identifier as the proposed value of F3 `reservations.code`, after confirming length and business-key policy. Never treat it as the integer primary key.

F2 emits whole-stay `total_amount`; F3 separately stores `first_night_amount` and `total_amount`. The first-night amount cannot be inferred accurately from total amount alone. Do not invent it. An approved rate lookup or explicit event field is required before inserting into the F3 operational table.

## Operational safety

- This migration is additive and must be reviewed before application.
- Use a local `.env` file or approved secret manager for Neon credentials. Never log or commit secrets.
- Test against an isolated development branch before `production`.
- Do not modify RabbitMQ configuration, queues, or Docker volumes as part of the Neon sink work.
- The lakehouse does not read Kafka: the Airflow worker bronze extracts `reservation_event_ledger` from Neon into S3 Bronze incrementally by `received_at`.
