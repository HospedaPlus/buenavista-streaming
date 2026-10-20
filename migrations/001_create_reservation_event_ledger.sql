-- Additive event ledger for idempotent Kafka-to-Neon ingestion.
-- This migration does not alter the existing F3 operational tables.
BEGIN;

CREATE TABLE IF NOT EXISTS reservation_event_ledger (
    event_id UUID PRIMARY KEY,
    reservation_id VARCHAR(80) NOT NULL,
    reservation_version BIGINT NOT NULL,
    schema_version VARCHAR(20) NOT NULL,
    source VARCHAR(80) NOT NULL,
    event_type VARCHAR(40) NOT NULL,
    occurred_at TIMESTAMPTZ NOT NULL,
    received_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    payload JSONB NOT NULL,
    kafka_topic VARCHAR(249) NOT NULL,
    kafka_partition INTEGER NOT NULL,
    kafka_offset BIGINT NOT NULL,

    CONSTRAINT uq_reservation_event_version
        UNIQUE (reservation_id, reservation_version),
    CONSTRAINT uq_reservation_event_kafka_position
        UNIQUE (kafka_topic, kafka_partition, kafka_offset),
    CONSTRAINT ck_reservation_event_version
        CHECK (reservation_version >= 1),
    CONSTRAINT ck_reservation_event_partition
        CHECK (kafka_partition >= 0),
    CONSTRAINT ck_reservation_event_offset
        CHECK (kafka_offset >= 0)
);

CREATE INDEX IF NOT EXISTS idx_reservation_event_history
    ON reservation_event_ledger (reservation_id, reservation_version DESC);

CREATE INDEX IF NOT EXISTS idx_reservation_event_received_at
    ON reservation_event_ledger (received_at);

COMMIT;
