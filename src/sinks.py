"""Idempotent Neon and Bronze sinks for synthetic F2 events."""
import json
from datetime import UTC
from pathlib import Path
from typing import Any
import boto3
import psycopg
from src.config import Settings
from src.events import ReservationEvent

class EventSinks:
    def __init__(self, settings: Settings):
        if not settings.database_url:
            raise ValueError("DATABASE_URL is required by the F2 consumer")
        self.settings = settings
        self.s3 = boto3.client("s3") if settings.bronze_bucket else None

    def write(self, event: ReservationEvent) -> None:
        """Write Bronze first, then Neon; replay is safe because keys are event_id."""
        raw = event.model_dump_json()
        self._write_bronze(event, raw)
        self._write_neon(event, raw)

    def _write_bronze(self, event: ReservationEvent, raw: str) -> None:
        event_day = event.occurred_at.astimezone(UTC).date().isoformat()
        key = (f"{self.settings.bronze_prefix.rstrip('/')}/event_date={event_day}/"
               f"event_type={event.event_type}/event_id={event.event_id}.json")
        if self.s3:
            self.s3.put_object(
                Bucket=self.settings.bronze_bucket, Key=key, Body=raw.encode("utf-8"),
                ContentType="application/json",
                Metadata={"source": event.source, "schema-version": event.schema_version},
            )
            return
        path = Path(self.settings.bronze_local_dir) / f"event_date={event_day}" / event.event_type
        path.mkdir(parents=True, exist_ok=True)
        (path / f"event_id={event.event_id}.json").write_text(raw + "\n", encoding="utf-8")

    def _write_neon(self, event: ReservationEvent, raw: str) -> None:
        payload: dict[str, Any] = event.data.model_dump(mode="json")
        with psycopg.connect(self.settings.database_url) as conn:
            with conn.cursor() as cur:
                cur.execute(
                    """INSERT INTO streaming.f2_events
                       (event_id, schema_version, source, event_type, occurred_at, hotel_id,
                        reservation_id, reservation_version, data, raw_event)
                       VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s::jsonb, %s::jsonb)
                       ON CONFLICT (event_id) DO NOTHING""",
                    (str(event.event_id), event.schema_version, event.source, event.event_type,
                     event.occurred_at, event.hotel_id, event.reservation_id,
                     event.reservation_version, json.dumps(payload), raw),
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
                     str(event.event_id), event.event_type, event.occurred_at, json.dumps(payload)),
                )
