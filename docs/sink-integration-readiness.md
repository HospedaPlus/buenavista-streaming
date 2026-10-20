# Kafka sink integration readiness

## Verified contracts

The current F2 Kafka event contract is schema version `1.1`. Each event has a
stable UUID `event_id`, `reservation_id`, a per-reservation `reservation_version`,
`occurred_at`, `hotel_id`, event type, source, and a reservation snapshot. The
event generator intentionally allows delivery out of order. Consumers must
therefore deduplicate by `event_id` and apply reservation state by version, not
arrival order. See [event-contract.md](event-contract.md).

The shared `db_simulated` repository defines the current F3 operational schema.
Its `reservations` table has an integer identity key, unique `code`, foreign
keys to hotel/room type/channel/guest, `adults`/`children`/`infants`,
`first_night_amount`, and a restricted status set (`confirmed`, `cancelled`,
`no_show`). It has no F2 event ledger or event ID/version columns. F2's
reservation snapshot instead uses a string reservation ID, `guests`,
`total_amount`, lowercase room type values, and lifecycle states including
`modified`, `checked_in`, and `checked_out`. A direct upsert into F3's
operational `reservations` table is not defined by the current contracts.

The F3 repository documents a shared Neon PostgreSQL 16 database and states
that credentials must be obtained from the team and never committed. Its seed
commands can truncate and reload the database; the streaming consumer must not
invoke those commands. The shared catalog is canonical and must be aligned from
its source rather than independently edited here.

The migration architecture requires separate Kafka consumer groups for Neon
and S3, at-least-once delivery, idempotent destination writes, and offset
commits only after each destination succeeds. The current Kafka validation
consumer only logs validated events; production sink retries and a Kafka DLQ
are not implemented. Existing RabbitMQ retry/DLQ handling is separate and
must remain untouched.

## Decisions needed before sink writes

1. **Neon mapping:** choose whether F2 events are an append-only event ledger,
   a projection into a new F2-specific table, or updates to the F3 operational
   reservations model. Define how hotel, room type, and channel codes map to
   F3 foreign keys, how guest counts and first-night amounts are derived, and
   how `modified`/`checked_in`/`checked_out` map to the target lifecycle.
2. **Neon idempotency and ordering:** approve a unique event key (normally
   `event_id`) and transaction/upsert rules. Define how stale or duplicate
   `reservation_version` values affect the projection. No such uniqueness or
   version columns currently exist in F3's `reservations` table.
3. **S3 Bronze:** provide bucket, region/account, encryption and access
   policy, retention, serialization/compression, object-key/partition layout,
   and whether a repeated event replaces the same object or creates a new
   version. Include the required ingestion timestamp/metadata and decide how
   schema versions are retained.
4. **Retry and dead-letter policy:** approve retry topic names/topology,
   maximum attempts, delay/backoff, transient versus permanent error rules,
   DLQ naming/retention, and the required error metadata and replay procedure.
   Kafka retry topics need an explicit scheduling strategy; RabbitMQ's current
   TTL queue settings cannot be assumed to apply.
5. **Runtime configuration and operations:** supply secret names/credential
   delivery method (not secret values in source control), TLS/authentication
   requirements, consumer group IDs, deployment owner, health/lag alerts,
   and recovery/replay expectations. Confirm local versus production targets.
6. **Source alignment:** confirm which shared catalog version the event
   generator must use and approve alignment of room-type/channel codes and
   reservation IDs with `db_simulated`.

## Safe implementation sequence

After the decisions above are approved, implement a shared Kafka delivery
loop with independently configured Neon and S3 sink adapters. Validate the
event before dispatch; route invalid/permanent failures to the approved DLQ;
retry transient failures according to the approved bounded policy; commit
that sink group's offset only after an idempotent sink success or a confirmed
DLQ transfer. Add unit tests for duplicate events, out-of-order reservation
versions, retry exhaustion, DLQ publication failure, and commit ordering, then
run isolated integration tests against disposable Kafka/PostgreSQL/S3 test
targets. Do not claim cross-system exactly-once delivery.

Until the mapping and destination policies are approved, writing either sink
would require inventing schema, object layout, or retry semantics and is
intentionally blocked.
