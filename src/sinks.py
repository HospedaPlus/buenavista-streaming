"""Neon OLTP projector for the F2 Kafka consumer group."""
import json
import psycopg
from src.config import Settings
from src.events import ReservationEvent

class NeonProjector:
    """Persist an F2 inbox and monotonic current-state projection in one DB transaction."""
    def __init__(self, settings: Settings):
        if not settings.database_url:
            raise ValueError("DATABASE_URL is required by the Neon OLTP consumer")
        self.conn = psycopg.connect(settings.database_url)

    def apply(self, event: ReservationEvent) -> None:
        data = event.data.model_dump(mode="json")
        raw_event = event.model_dump_json()
        # The event inbox insert, projection upsert, and Kafka offset commit are
        # deliberately ordered: this transaction must succeed before commit.
        with self.conn.transaction():
            with self.conn.cursor() as cur:
                cur.execute(
                    """INSERT INTO streaming.f2_event_inbox
                       (event_id, schema_version, source, event_type, occurred_at, hotel_id,
                        reservation_id, reservation_version, data, raw_event)
                       VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s::jsonb, %s::jsonb)
                       ON CONFLICT (event_id) DO NOTHING""",
                    (str(event.event_id), event.schema_version, event.source, event.event_type,
                     event.occurred_at, event.hotel_id, event.reservation_id,
                     event.reservation_version, json.dumps(data), raw_event),
                )
                cur.execute(
                    """INSERT INTO streaming.f2_reservations_current
                       (reservation_id, hotel_id, reservation_version, event_id, event_type,
                        occurred_at, data, updated_at)
                       VALUES (%s, %s, %s, %s, %s, %s, %s::jsonb, now())
                       ON CONFLICT (reservation_id) DO UPDATE SET
                         hotel_id = EXCLUDED.hotel_id,
                         reservation_version = EXCLUDED.reservation_version,
                         event_id = EXCLUDED.event_id, event_type = EXCLUDED.event_type,
                         occurred_at = EXCLUDED.occurred_at, data = EXCLUDED.data,
                         updated_at = now()
                       WHERE EXCLUDED.reservation_version > streaming.f2_reservations_current.reservation_version""",
                    (event.reservation_id, event.hotel_id, event.reservation_version,
                     str(event.event_id), event.event_type, event.occurred_at, json.dumps(data)),
                )

    def close(self) -> None:
        self.conn.close()
