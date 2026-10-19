# HospedaPlus F2 Kafka Data Flow

Synthetic F2 reservation events flow through Kafka into independent OLTP and OLAP paths, following the tech lead's approved architecture. The repository supplies the F2 producer, Neon projection consumer, and S3 Bronze capture consumer. It does not deploy the production services.

## Approved flow

```
F2 channel-manager simulator
  → Python event producer (validate v1.1, synthetic/no guest PII)
  → Kafka: buenavista.reservations.v1 (key = reservation_id)
      ├─ OLTP group → Neon PostgreSQL → streaming.f2_event_inbox
      │                                  → streaming.f2_reservations_current
      │                                  → booking and operations APIs
      └─ OLAP group → S3 Bronze raw events + event/ingest times + Kafka coordinates
                         → Airflow scheduled batch transforms
                         → Silver (validate, deduplicate, pseudonymize)
                         → Gold revenue facts and dimensions
                         → Power BI dashboards

Kafka retry topic → bounded retry attempts → DLQ for invalid/exhausted events
```

## Melissa's F3 database contract

The local hotel ID list contains the same 28 hotel codes as the canonical catalog in [HospedaPlus/db_simulated](https://github.com/HospedaPlus/db_simulated/tree/main/catalog), whose owner is the F3/database workstream. Keep the shared catalog authoritative. F2 events are a separate stream projection; do not insert them into the existing F3 `reservations` table or overwrite any F3 schema.

`sql/streaming_schema.sql` proposes the additive `streaming` schema with `f2_event_inbox` and `f2_reservations_current`. The Neon OLTP group writes both in one transaction and commits the Kafka offset only after success. The S3 Bronze group has a separate consumer group and commits only after its append-only raw record is persisted.

## Start locally

Follow [docs/runbook.md](docs/runbook.md). The local KRaft broker is for development only. Configure Neon and S3 credentials through environment variables/AWS credential providers; never commit credentials. Airflow Silver/Gold processing belongs to the data-platform deployment.

## Scope and delivery semantics

Kafka gives each consumer group an independent view of the topic. Processing is at-least-once: consumers may replay after failures, so the OLTP inbox deduplicates by event ID and the Bronze key includes Kafka coordinates. The local single-node topology, per-record Bronze writes, and unauthenticated broker are a demonstrator, not a production-ready or cost-validated deployment.
