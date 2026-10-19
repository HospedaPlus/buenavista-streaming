# HospedaPlus F2 Kafka Streaming

F2 simulates Channel Manager reservation lifecycle events and publishes schema v1.1 messages to Kafka. This repository implements the producer, the independent Neon OLTP projection consumer, and the independent Amazon S3 Bronze OLAP capture consumer. It follows the approved tech-lead flow; it is not a production deployment yet.

## Approved architecture

```text
Channel Manager simulator
  → Python producer (schema v1.1; synthetic data; no guest PII)
  → Kafka topic buenavista.reservations.v1 (key = reservation_id)
      ├─ OLTP group → Neon PostgreSQL
      │                 ├─ streaming.f2_event_inbox (event_id deduplication)
      │                 └─ streaming.f2_reservations_current (newest reservation_version)
      │                 → booking and operations APIs
      └─ OLAP group → S3 Bronze (raw event, event/ingest times, Kafka coordinates)
                         → Airflow scheduled transforms
                         → Silver: validate, deduplicate, pseudonymize
                         → Gold: revenue facts and dimensions
                         → Power BI
  Invalid/exhausted OLTP records → retry topic → DLQ
```

The approved diagram's Bronze partitions are `ingest_date` and `hotel_id`. Each consumer group has an independent Kafka offset. Processing is at-least-once: Neon deduplicates by `event_id`; Bronze records retain topic, partition, and offset for replay reconciliation. OLTP offsets are committed after the database transaction; Bronze offsets are committed after all objects in its micro-batch are written.

## Data ownership and contracts

- The F2 event envelope remains schema v1.1 as approved. Its guest and booking values are synthetic; direct guest identifiers and payment data are excluded.
- The 28 hotel IDs match Melissa's canonical catalog in [HospedaPlus/db_simulated](https://github.com/HospedaPlus/db_simulated/tree/main/catalog). Keep that catalog authoritative. Align channel and room-type values with the F3 catalog before building analytics joins.
- F2 writes only to the additive `streaming` schema proposed in `sql/streaming_schema.sql`. Existing F3 tables, including its `reservations` table, remain owned by the database workstream. The SQL file is a migration proposal and must be reviewed and applied through the team's migration process.
- API availability/current-rate endpoints, Airflow transforms, Silver/Gold models, and Power BI remain handoffs to their owning workstreams; they are not implemented here.

## Run locally

Use [docs/runbook.md](docs/runbook.md). The local Docker Compose Kafka broker is a single-node KRaft development broker with PLAINTEXT security. It is not an AWS or production topology. A local smoke run requires Docker Compose and Python 3.12+.

## Deployment migration status

The migration starts with a container image and production Kafka TLS/SASL client configuration. See [deploy/aws/README.md](deploy/aws/README.md) for the proposed AWS run path, required decisions, staged rollout, and validation gates.

- Runtime supports TLS/SASL configuration through environment variables. Production credentials must be provided at runtime from the approved secret store; do not commit them.
- AWS task roles should grant only the required S3 prefix writes and secret reads. Neon remains an external managed PostgreSQL destination reached over TLS.
- The proposed AWS shape is managed Kafka (MSK) plus separate ECS/Fargate tasks for the simulator, Neon projector, and Bronze capture, with S3 as the Bronze destination. The exact MSK mode, network, credentials mechanism, sizing, and cost must be validated before provisioning.
- The Docker image supports the producer by default. Consumer task commands are `python -m src.consumer` and `python -m src.bronze_consumer`.
- Deployment, Neon schema application, and production credentials are not performed by this PR.

## Team documentation handoff

Keep implementation docs in this repository aligned with the approved diagram and contracts. Vane is responsible for uploading the agreed documentation changes to Docusaurus. Coordinate the shared channel and room-type catalog with Oscar, Melissa, and Johana before treating those values as an analytics contract.

## Delivery boundaries

The local single-node broker is a demonstrator. This repository does not claim production readiness, a cost estimate, or guaranteed business outcomes. Production rollout requires the security, infrastructure, catalog, migration, monitoring, and recovery gates in the AWS deployment guide.
