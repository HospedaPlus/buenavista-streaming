# HospedaPlus Buenavista Streaming (F2)

Synthetic F2 channel-manager lifecycle event producer and Kafka ingestion pipeline. This repository demonstrates the proposed event flow; it is not a production deployment.

## Flow

```
Synthetic F2 source
  → Kafka topic (key = reservation_id)
  → consumer group
       ├─ raw JSON → S3 Bronze (or local Bronze for development)
       └─ Neon PostgreSQL: streaming.f2_events + streaming.f2_reservations_current
  → Airflow can later read Bronze into Silver/Gold
```

Kafka is the only message broker. F2's event tables are isolated in the additive `streaming` schema and do not replace or write to Melissa's F3 `reservations` tables. The canonical shared catalog is maintained by [db_simulated](https://github.com/HospedaPlus/db_simulated/tree/main/catalog); this repo's hotel ID file is a pinned copy, not an independent catalog.

## Event simulator

Generates synthetic events for the existing 28-hotel catalog, including created, updated, cancelled, no-show, checked-in, and checked-out lifecycle events. The v1.1 contract includes no direct guest identifiers. The simulator defaults to a 37% cancellation outcome and can produce intentionally out-of-order lifecycle events; a consumer must reconcile with `reservation_version`.

## Quick start

See [the Kafka runbook](docs/runbook.md). Start the local KRaft broker, create the main and DLQ topics, configure a disposable PostgreSQL database, apply [the additive F2 schema](sql/streaming_schema.sql), then run the producer and consumer.

- `python -m src.producer --count 20 --seed 42`
- `python -m src.consumer`

The local broker has no authentication and is for development only. A local Bronze directory is used when `BRONZE_S3_BUCKET` is empty. Configure the approved Neon URL and AWS credentials through environment configuration for the intended destinations.

## Delivery and recovery

The producer uses Kafka acknowledgements and idempotence. Consumer offsets are committed after Bronze and Neon writes; the sinks are idempotent by `event_id`, while the current projection accepts only a higher `reservation_version`. Invalid records are routed to a DLQ topic. Kafka-to-Neon/S3 is at-least-once across systems, not a distributed exactly-once transaction.

## Database safety

`sql/streaming_schema.sql` only creates F2-owned objects under `streaming`. It is a proposed migration; have the database owner review and apply it. No production database access or migration is performed by this repository change. Never run the F3 seed/reset workflow against shared Neon.

## Scope limits

This is an end-to-end demonstrator, not a production-ready system. Production requires approved broker security, AWS IAM, partition/retention sizing, observability, alerting, schema governance, and load/recovery validation. The source repository provides no credentials or AWS account details, so those are supplied at deployment time.
