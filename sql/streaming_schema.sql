-- Additive F2-owned OLTP objects. F3 tables remain unchanged.
-- Review and apply through the team's approved Neon migration process.
CREATE SCHEMA IF NOT EXISTS streaming;

CREATE TABLE IF NOT EXISTS streaming.f2_event_inbox (
    event_id uuid PRIMARY KEY,
    schema_version text NOT NULL,
    source text NOT NULL CHECK (source = 'f2-channel-manager'),
    event_type text NOT NULL,
    occurred_at timestamptz NOT NULL,
    hotel_id text NOT NULL,
    reservation_id text NOT NULL,
    reservation_version integer NOT NULL CHECK (reservation_version >= 1),
    data jsonb NOT NULL,
    raw_event jsonb NOT NULL,
    ingested_at timestamptz NOT NULL DEFAULT now()
);
CREATE INDEX IF NOT EXISTS f2_event_inbox_hotel_time_idx
    ON streaming.f2_event_inbox (hotel_id, occurred_at);
CREATE INDEX IF NOT EXISTS f2_event_inbox_reservation_version_idx
    ON streaming.f2_event_inbox (reservation_id, reservation_version);

CREATE TABLE IF NOT EXISTS streaming.f2_reservations_current (
    reservation_id text PRIMARY KEY,
    hotel_id text NOT NULL,
    reservation_version integer NOT NULL CHECK (reservation_version >= 1),
    event_id uuid NOT NULL,
    event_type text NOT NULL,
    occurred_at timestamptz NOT NULL,
    data jsonb NOT NULL,
    updated_at timestamptz NOT NULL DEFAULT now()
);
CREATE INDEX IF NOT EXISTS f2_current_hotel_idx
    ON streaming.f2_reservations_current (hotel_id);
