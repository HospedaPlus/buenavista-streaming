"""Persist validated Kafka reservation events to the additive Neon event ledger.

This module stores immutable event history and Kafka provenance. It does not
modify the existing F3 operational reservations table.
"""

import os
from typing import Any

import psycopg
from dotenv import load_dotenv
from psycopg.types.json import Jsonb

from src.events import ReservationEvent

# Load local development settings without overriding variables supplied by the
# process or deployment platform.
load_dotenv()


class NeonEventSink:
    """Persist reservation events idempotently in PostgreSQL on Neon."""

    def __init__(self, database_url: str | None = None) -> None:
        """Use an explicit connection URL or the NEON_DATABASE_URL environment variable."""
        self.database_url = database_url or os.getenv("NEON_DATABASE_URL")
        if not self.database_url:
            raise ValueError("NEON_DATABASE_URL must be configured in the environment or .env file")
        # One connection is reused for every event instead of opening a new one
        # per message (at 10-15 events/s that saves a TLS handshake each time).
        self._connection: psycopg.Connection | None = None

    def _connect(self) -> psycopg.Connection:
        """Return the open connection, reconnecting if it was closed or broken."""
        if self._connection is None or self._connection.closed:
            # autocommit=True + "with connection.transaction()" in _persist gives one
            # explicit transaction per event.
            self._connection = psycopg.connect(self.database_url, autocommit=True)
        return self._connection

    def close(self) -> None:
        """Close the database connection, if any."""
        if self._connection is not None:
            self._connection.close()
            self._connection = None

    def persist(
        self,
        event: ReservationEvent,
        topic: str,
        partition: int,
        offset: int,
    ) -> bool:
        """Write one event and its Kafka provenance in a single transaction.

        Returns True when a new row is inserted and False when the same event
        and payload have already been stored. Reusing an event ID for a
        different payload raises ValueError instead of silently hiding a
        producer or data-integrity defect.

        The Kafka offset must only be committed after this method returns.
        """
        payload: dict[str, Any] = event.model_dump(mode="json")

        connection = self._connect()
        try:
            return self._persist(connection, event, payload, topic, partition, offset)
        except psycopg.OperationalError:
            # Broken connection: drop it so the next call reconnects. The error is
            # re-raised so the caller does not commit the Kafka offset.
            self.close()
            raise

    @staticmethod
    def _persist(connection, event, payload, topic, partition, offset) -> bool:
        # The transaction block commits on success and rolls back if an exception
        # escapes it. No F3 operational table is touched.
        with connection.transaction():
            with connection.cursor() as cursor:
                cursor.execute(
                    """
                    INSERT INTO public.reservation_event_ledger (
                        event_id, reservation_id, reservation_version,
                        schema_version, source, event_type, occurred_at,
                        payload, kafka_topic, kafka_partition, kafka_offset
                    )
                    VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
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
                inserted = cursor.fetchone() is not None

                if not inserted:
                    # A duplicate is safe only when its original payload
                    # matches. A conflicting payload must stop processing.
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
                            f"Event ID conflict with different payload: {event.event_id}"
                        )

                return inserted
