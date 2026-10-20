"""Persist validated Kafka reservation events to the Neon event ledger.

This module stores event history and Kafka provenance.
It does not modify the operational reservations table.
"""

import os
from typing import Any

import psycopg
from dotenv import load_dotenv
from psycopg.types.json import Jsonb

from src.events import ReservationEvent

# Load environment variables from the project's .env file.
# Existing system environment variables remain available.
load_dotenv()


class NeonEventSink:
    """Persist validated reservation events in PostgreSQL."""

    def __init__(self, database_url: str | None = None) -> None:
        """Initialize the sink using an explicit URL or environment settings."""
        self.database_url = database_url or os.getenv("NEON_DATABASE_URL")

        if not self.database_url:
            raise ValueError(
                "NEON_DATABASE_URL must be configured in the environment or .env file"
            )

    def persist(
        self,
        event: ReservationEvent,
        topic: str,
        partition: int,
        offset: int,
    ) -> bool:
        """Persist one event and its Kafka provenance.

        Returns:
            True if a new event was inserted.
            False if the same event ID and payload were already stored.

        Raises:
            ValueError: If an existing event ID has a different payload.
            psycopg.Error: If the database operation fails.

        The database transaction commits on success and rolls back on error.
        Kafka offsets must only be committed after this method succeeds.
        """
        # Serialize the validated event into JSON-compatible Python values.
        payload: dict[str, Any] = event.model_dump(mode="json")

        # The connection context commits on success and rolls back on exceptions.
        with psycopg.connect(self.database_url) as connection:
            with connection.cursor() as cursor:
                # Insert the event once, using event_id as the idempotency key.
                # Store the Kafka topic, partition, and offset for traceability.
                cursor.execute(
                    """
                    INSERT INTO public.reservation_event_ledger (
                        event_id,
                        reservation_id,
                        reservation_version,
                        schema_version,
                        source,
                        event_type,
                        occurred_at,
                        payload,
                        kafka_topic,
                        kafka_partition,
                        kafka_offset
                    )
                    VALUES (
                        %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s
                    )
                    ON CONFLICT (event_id) DO NOTHING
                    RETURNING event_id
                    """,
                    (
                        str(event.event_id),
                        event.reservation_id,
                        event.reservation_version,
                        event.schema_version,
                        event.source,
                        event.event_type,
                        event.occurred_at,
                        Jsonb(payload),
                        topic,
                        partition,
                        offset,
                    ),
                )

                # A returned row means this event was inserted for the first time.
                inserted = cursor.fetchone() is not None

                if not inserted:
                    # Verify that a duplicate event ID carries the same payload.
                    # A different payload under the same ID indicates a conflict.
                    cursor.execute(
                        """
                        SELECT payload
                        FROM public.reservation_event_ledger
                        WHERE event_id = %s
                        """,
                        (str(event.event_id),),
                    )
                    existing = cursor.fetchone()

                    if existing is None or existing[0] != payload:
                        raise ValueError(
                            "Event ID conflict with different payload: "
                            f"{event.event_id}"
                        )

                # The transaction commits when the connection context exits.
                return inserted
