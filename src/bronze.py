"""Append-only S3 Bronze writer retaining event time and Kafka coordinates."""
import json
from datetime import UTC, datetime
from pathlib import Path
import boto3
from src.config import Settings


class BronzeWriter:
    def __init__(self, settings: Settings):
        self.settings = settings
        self.s3 = boto3.client("s3") if settings.bronze_bucket else None

    def write(self, raw_event: bytes, topic: str, partition: int, offset: int,
              kafka_timestamp_ms: int | None) -> str:
        now = datetime.now(UTC)
        raw_text = raw_event.decode("utf-8", errors="replace")
        try:
            event = json.loads(raw_text)
        except json.JSONDecodeError:
            event = None
        event_object = event if isinstance(event, dict) else None
        event_time = event_object.get("occurred_at") if event_object else None
        raw_value = event if event is not None else raw_text
        record = {
            "event_time": event_time,
            "ingest_time": now.isoformat(),
            "kafka": {
                "topic": topic,
                "partition": partition,
                "offset": offset,
                "timestamp_ms": kafka_timestamp_ms,
            },
            "raw_event": raw_value,
        }
        body = json.dumps(record, ensure_ascii=False, separators=(",", ":")) + "\n"
        hotel_id = event_object.get("hotel_id", "unknown") if event_object else "unknown"
        key = (f"{self.settings.bronze_prefix.rstrip('/')}/ingest_date={now.date().isoformat()}/"
               f"hotel_id={hotel_id}/topic={topic}/partition={partition}/offset={offset}.jsonl")
        if self.s3:
            self.s3.put_object(Bucket=self.settings.bronze_bucket, Key=key,
                               Body=body.encode("utf-8"), ContentType="application/x-ndjson")
        else:
            path = (
                Path(self.settings.bronze_local_dir)
                / f"ingest_date={now.date().isoformat()}"
                / f"hotel_id={hotel_id}"
                / f"partition={partition}"
            )
            path.mkdir(parents=True, exist_ok=True)
            (path / f"offset={offset}.jsonl").write_text(body, encoding="utf-8")
        return key
