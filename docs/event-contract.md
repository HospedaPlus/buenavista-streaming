# Reservation event contract

**Status:** F2 synthetic event contract v1.1. The Kafka migration preserves this envelope.

## Source alignment

The hotel IDs in `src/catalog/hotel_ids.json` are the 28-code pinned copy of the canonical shared catalog in [HospedaPlus/db_simulated](https://github.com/HospedaPlus/db_simulated/tree/main/catalog). Keep that source authoritative; update the local list only when the shared catalog is formally refreshed. F2 records remain separate from Melissa's F3 `reservations` table.

Events are synthetic and contain no guest identifiers or payment data.

## Envelope and payload

Fields: `event_id` UUID, `schema_version` = `1.1`, `source` = `f2-channel-manager`, `event_type`, UTC `occurred_at`, `hotel_id`, `reservation_id`, `reservation_version`, and `data`.

Payload fields: `check_in`, `check_out`, `room_type`, `guests`, `total_amount`, `currency`, `status`, `channel`, `refundable`. Event types are created, updated, cancelled, no-show, checked-in, checked-out. The current v1.1 event model must be coordinated with the data-platform owner before any contract version change.

## Kafka flow

- Main topic: `buenavista.reservations.v1`; key: `reservation_id`; retry topic: `buenavista.reservations.retry.v1`; DLQ: `buenavista.reservations.dlq.v1`.
- The producer uses `acks=all` and idempotence. Kafka keying preserves a reservation's partition order; lifecycle versioning handles intentionally out-of-order simulated events.
- The OLTP consumer group writes `streaming.f2_event_inbox` and updates `streaming.f2_reservations_current` in one Neon transaction, then commits the Kafka offset. It does not write to F3-owned tables.
- The independent OLAP consumer group writes append-only raw events to S3 Bronze with event time, ingestion time, and Kafka topic/partition/offset. S3 keys are partitioned by `ingest_date` and `hotel_id`; offsets commit after the Bronze write.
- Invalid and repeatedly failing records go to retry/dead-letter topics. Duplicate delivery remains possible across systems; writes use event IDs and Kafka coordinates for idempotency.

## Downstream analytics

Airflow scheduled batch jobs consume Bronze into Silver, where they validate/deduplicate and pseudonymize before analytics, then publish Gold revenue facts and dimensions for Power BI. These downstream transforms are outside this streaming repository's runtime.

Apply `sql/streaming_schema.sql` only through the approved Neon migration process. Never run F3 seed/reset workflows against the shared Neon database.
