-- Additive F2-owned schema. Never apply the F3 seed/reset workflow to Neon.
-- Apply once with an approved migration role after review by the database owner.
CREATE SCHEMA IF NOT EXISTS streaming;

CREATE TABLE IF NOT EXISTS streaming.f2_events (
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
CREATE INDEX IF NOT EXISTS f2_events_hotel_occurred_idx
    ON streaming.f2_events (hotel_id, occurred_at);
CREATE INDEX IF NOT EXISTS f2_events_reservation_version_idx
    ON streaming.f2_events (reservation_id, reservation_version);

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
