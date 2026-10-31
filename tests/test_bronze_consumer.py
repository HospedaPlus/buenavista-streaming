"""Tests for the S3 Bronze landing consumer: object layout and commit ordering."""

import gzip
import json
from datetime import UTC, datetime
from unittest.mock import MagicMock

import pytest

from src import bronze_consumer
from src.events import build_event


def kafka_message(offset, partition=0):
    event = build_event(index=offset, seed=42)
    message = MagicMock()
    message.error.return_value = None
    message.topic.return_value = "reservation.events.v1_3"
    message.partition.return_value = partition
    message.offset.return_value = offset
    message.key.return_value = event.reservation_id.encode()
    message.value.return_value = event.model_dump_json().encode()
    return message


def test_object_key_is_partitioned_by_ingestion_date_and_hour():
    records = [{"kafka_offset": 7}, {"kafka_offset": 42}]
    key = bronze_consumer.object_key(
        "bronze/f2", "reservation.events.v1_3", 2, records, datetime(2026, 11, 3, 14, 5, tzinfo=UTC)
    )
    assert key == (
        "bronze/f2/ingest_date=2026-11-03/hour=14/"
        "reservation.events.v1_3-p2-000000000007-000000000042.jsonl.gz"
    )


def test_batch_is_written_as_gzip_json_lines_with_raw_value():
    s3 = MagicMock()
    at = datetime(2026, 11, 3, 14, tzinfo=UTC)
    records = [bronze_consumer.to_bronze_record(kafka_message(i), at) for i in range(3)]

    written = bronze_consumer.write_batch(s3, "lake", "bronze/f2", {0: records}, at)

    assert written == 1
    body = s3.put_object.call_args.kwargs["Body"]
    lines = gzip.decompress(body).decode().splitlines()
    assert [json.loads(line)["kafka_offset"] for line in lines] == [0, 1, 2]
    assert json.loads(json.loads(lines[0])["raw_value"])["schema_version"] == "1.3"


def test_offsets_are_committed_only_after_s3_write(monkeypatch):
    monkeypatch.setenv("BRONZE_BUCKET", "lake")
    monkeypatch.setenv("BRONZE_MAX_BATCH", "2")
    monkeypatch.setattr(bronze_consumer.signal, "signal", MagicMock())
    consumer = MagicMock()
    s3 = MagicMock()
    actions = []
    messages = [kafka_message(0), kafka_message(1)]

    def poll(_timeout):
        if messages:
            return messages.pop(0)
        bronze_consumer.shutdown_requested = True
        return None

    consumer.poll.side_effect = poll
    s3.put_object.side_effect = lambda **_: actions.append("s3")
    consumer.commit.side_effect = lambda **_: actions.append("commit")
    monkeypatch.setattr(bronze_consumer, "Consumer", MagicMock(return_value=consumer))

    bronze_consumer.consume_to_bronze(s3=s3)

    assert actions == ["s3", "commit"]


def test_failed_s3_write_does_not_commit(monkeypatch):
    monkeypatch.setenv("BRONZE_BUCKET", "lake")
    monkeypatch.setenv("BRONZE_MAX_BATCH", "1")
    monkeypatch.setattr(bronze_consumer.signal, "signal", MagicMock())
    consumer = MagicMock()
    consumer.poll.return_value = kafka_message(0)
    s3 = MagicMock()
    s3.put_object.side_effect = RuntimeError("S3 unavailable")
    monkeypatch.setattr(bronze_consumer, "Consumer", MagicMock(return_value=consumer))

    with pytest.raises(RuntimeError, match="S3 unavailable"):
        bronze_consumer.consume_to_bronze(s3=s3)

    consumer.commit.assert_not_called()
    consumer.close.assert_called_once_with()


def test_bucket_is_required(monkeypatch):
    monkeypatch.delenv("BRONZE_BUCKET", raising=False)
    with pytest.raises(ValueError, match="BRONZE_BUCKET"):
        bronze_consumer.consume_to_bronze(s3=MagicMock())
