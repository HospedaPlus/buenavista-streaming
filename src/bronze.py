"""Append-only S3 Bronze writer retaining event and Kafka metadata in micro-batches."""
import hashlib
import json
import re
from collections import defaultdict
from datetime import UTC, datetime
from pathlib import Path

import boto3

from src.config import Settings


class BronzeWriter:
    def __init__(self, settings: Settings):
        self.settings = settings
        self.s3 = boto3.client("s3") if settings.bronze_bucket else None

    def write_batch(self, messages) -> list[str]:
        """Write newline-delimited raw events grouped by ingest date and hotel."""
        now = datetime.now(UTC)
        groups = defaultdict(list)
        for message in messages:
            raw_text = message.value().decode("utf-8", errors="replace")
            try:
                event = json.loads(raw_text)
            except json.JSONDecodeError:
                event = None
            event_object = event if isinstance(event, dict) else None
            hotel_id = event_object.get("hotel_id", "unknown") if event_object else "unknown"
            if not re.fullmatch(r"HP-[A-Z]{2}-\d{3}", hotel_id):
                hotel_id = "unknown"
            record = {
                "event_time": event_object.get("occurred_at") if event_object else None,
                "ingest_time": now.isoformat(),
                "kafka": {
                    "topic": message.topic(),
                    "partition": message.partition(),
                    "offset": message.offset(),
                    "timestamp_ms": _kafka_timestamp(message),
                },
                "raw_event": event if event is not None else raw_text,
            }
            groups[(now.date().isoformat(), hotel_id)].append((message, record))

        keys = []
        for (ingest_date, hotel_id), records in groups.items():
            coordinates = sorted(
                f"{message.topic()}:{message.partition()}:{message.offset()}"
                for message, _record in records
            )
            batch_id = hashlib.sha256("|".join(coordinates).encode("utf-8")).hexdigest()[:20]
            key = (
                f"{self.settings.bronze_prefix.rstrip('/')}/ingest_date={ingest_date}/"
                f"hotel_id={hotel_id}/batch_id={batch_id}.jsonl"
            )
            body = "".join(
                json.dumps(record, ensure_ascii=False, separators=(",", ":")) + "\n"
                for _message, record in records
            )
            if self.s3:
                self.s3.put_object(
                    Bucket=self.settings.bronze_bucket,
                    Key=key,
                    Body=body.encode("utf-8"),
                    ContentType="application/x-ndjson",
                )
            else:
                path = (
                    Path(self.settings.bronze_local_dir)
                    / f"ingest_date={ingest_date}"
                    / f"hotel_id={hotel_id}"
                )
                path.mkdir(parents=True, exist_ok=True)
                (path / f"batch_id={batch_id}.jsonl").write_text(body, encoding="utf-8")
            keys.append(key)
        return keys


def _kafka_timestamp(message) -> int | None:
    timestamp = message.timestamp()
    return timestamp[1] if timestamp and timestamp[0] >= 0 else None
