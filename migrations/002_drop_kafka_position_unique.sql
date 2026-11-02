-- The Kafka position (topic, partition, offset) is provenance, not identity.
-- When the local Kafka volume is recreated, offsets start again at 0 and the
-- old UNIQUE constraint stopped the consumer with UniqueViolation.
-- event_id stays the idempotency key. Safe to re-run.
ALTER TABLE streaming.reservation_event_ledger
    DROP CONSTRAINT IF EXISTS uq_reservation_event_kafka_position;

CREATE INDEX IF NOT EXISTS idx_reservation_event_kafka_position
    ON streaming.reservation_event_ledger (kafka_topic, kafka_partition, kafka_offset);
