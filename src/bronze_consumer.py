"""Consume Kafka events and land them, as they arrive, in the S3 Bronze layer.

Bronze keeps the raw record exactly as it came from Kafka (no validation, no
transformation) plus its Kafka position, so Silver can always be rebuilt.

Processing order (at-least-once, nothing lost):
1. Poll messages and keep them in memory, grouped by Kafka partition.
2. Every BRONZE_FLUSH_SECONDS (default 300 s = 5 min) or BRONZE_MAX_BATCH
   messages, write one gzip JSON Lines object per partition to S3.
3. Commit the Kafka offsets only after every S3 write is confirmed.

If the process stops before step 3, the same messages are read again and
written again. Duplicates in Bronze are expected and removed in Silver by
``event_id``. Object names include the first and last offset, so a replay of the
same batch overwrites the same object instead of creating a new one.
"""

import gzip
import json
import logging
import os
import signal
import time
from collections import defaultdict
from collections.abc import Callable
from datetime import UTC, datetime
from typing import Any

import boto3
from confluent_kafka import Consumer, KafkaError, KafkaException

logging.basicConfig(
    level=os.getenv("LOG_LEVEL", "INFO").upper(),
    format="%(asctime)s %(levelname)s %(name)s %(message)s",
)
logger = logging.getLogger(__name__)
shutdown_requested = False
POLL_TIMEOUT_SECONDS = 1.0
DEFAULT_TOPIC = "reservation.events.v1_3"
DEFAULT_PREFIX = "bronze/f2_reservation_events"


def request_shutdown(signum: int, frame: Any) -> None:
    """Flush what is buffered and stop."""
    del frame
    global shutdown_requested
    shutdown_requested = True
    logger.info("Shutdown requested: signal=%s", signum)


def to_bronze_record(message, ingested_at: datetime) -> dict:
    """Wrap the raw Kafka value with its provenance. The value is not parsed."""
    return {
        "kafka_topic": message.topic(),
        "kafka_partition": message.partition(),
        "kafka_offset": message.offset(),
        "kafka_key": (message.key() or b"").decode("utf-8", errors="replace"),
        "ingested_at": ingested_at.isoformat(),
        "raw_value": (message.value() or b"").decode("utf-8", errors="replace"),
    }


def object_key(prefix: str, topic: str, partition: int, records: list[dict], at: datetime) -> str:
    """Return the S3 key, partitioned by ingestion date and hour (Hive style)."""
    first = records[0]["kafka_offset"]
    last = records[-1]["kafka_offset"]
    return (
        f"{prefix}/ingest_date={at:%Y-%m-%d}/hour={at:%H}/"
        f"{topic}-p{partition}-{first:012d}-{last:012d}.jsonl.gz"
    )


def write_batch(s3, bucket: str, prefix: str, batch: dict[int, list[dict]], at: datetime) -> int:
    """Write one gzip JSON Lines object per partition. Returns objects written."""
    written = 0
    for partition, records in sorted(batch.items()):
        if not records:
            continue
        body = gzip.compress(
            "".join(json.dumps(r, ensure_ascii=False) + "\n" for r in records).encode("utf-8")
        )
        key = object_key(prefix, records[0]["kafka_topic"], partition, records, at)
        extra = {}
        kms_key = os.getenv("BRONZE_KMS_KEY_ID")
        if kms_key:
            extra = {"ServerSideEncryption": "aws:kms", "SSEKMSKeyId": kms_key}
        s3.put_object(
            Bucket=bucket,
            Key=key,
            Body=body,
            ContentType="application/x-ndjson",
            ContentEncoding="gzip",
            **extra,
        )
        written += 1
        logger.info("Bronze object written s3://%s/%s records=%s", bucket, key, len(records))
    return written


def consume_to_bronze(
    s3=None,
    clock: Callable[[], float] = time.monotonic,
    now: Callable[[], datetime] = lambda: datetime.now(UTC),
) -> None:
    """Buffer Kafka messages and land them in S3, committing offsets after each flush."""
    global shutdown_requested
    shutdown_requested = False

    broker = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
    topic = os.getenv("KAFKA_RESERVATION_TOPIC", DEFAULT_TOPIC)
    group_id = os.getenv("KAFKA_BRONZE_GROUP_ID", "hospedaplus-bronze-s3")
    bucket = os.getenv("BRONZE_BUCKET")
    prefix = os.getenv("BRONZE_PREFIX", DEFAULT_PREFIX)
    flush_seconds = float(os.getenv("BRONZE_FLUSH_SECONDS", "300"))
    max_batch = int(os.getenv("BRONZE_MAX_BATCH", "5000"))
    if not bucket:
        raise ValueError("BRONZE_BUCKET must be configured in the environment or .env file")
    s3 = s3 or boto3.client("s3")

    consumer = Consumer(
        {
            "bootstrap.servers": broker,
            "group.id": group_id,
            "auto.offset.reset": "earliest",
            "enable.auto.commit": False,
        }
    )
    signal.signal(signal.SIGINT, request_shutdown)
    signal.signal(signal.SIGTERM, request_shutdown)
    consumer.subscribe([topic])
    logger.info("Bronze consumer started topic=%s group=%s bucket=%s", topic, group_id, bucket)

    batch: dict[int, list[dict]] = defaultdict(list)
    buffered = 0
    last_flush = clock()

    def flush() -> None:
        nonlocal batch, buffered, last_flush
        if buffered:
            write_batch(s3, bucket, prefix, batch, now())
            # Every buffered message is now in S3, so committing the consumer's
            # current positions is safe.
            consumer.commit(asynchronous=False)
        batch = defaultdict(list)
        buffered = 0
        last_flush = clock()

    try:
        while not shutdown_requested:
            message = consumer.poll(POLL_TIMEOUT_SECONDS)
            if message is not None:
                if message.error():
                    if message.error().code() != KafkaError._PARTITION_EOF:
                        raise KafkaException(message.error())
                else:
                    batch[message.partition()].append(to_bronze_record(message, now()))
                    buffered += 1
            if buffered >= max_batch or clock() - last_flush >= flush_seconds:
                flush()
        flush()
    finally:
        consumer.close()
        logger.info("Bronze consumer stopped")


if __name__ == "__main__":
    consume_to_bronze()
