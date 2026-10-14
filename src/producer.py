"""Publish synthetic F2 reservation events to Amazon Kinesis Data Streams."""

import argparse
import logging
import time

import boto3
from botocore.exceptions import ClientError

from src.config import get_settings
from src.events import build_event

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
logger = logging.getLogger(__name__)


def create_client():
    settings = get_settings()
    kwargs = {
        "service_name": "kinesis",
        "region_name": settings.region,
        "aws_access_key_id": settings.access_key_id,
        "aws_secret_access_key": settings.secret_access_key,
    }
    if settings.endpoint_url:
        kwargs["endpoint_url"] = settings.endpoint_url
    return boto3.client(**kwargs)


def ensure_stream(client, stream_name: str, shard_count: int) -> None:
    """Create the stream if missing and wait until it becomes active."""
    try:
        client.describe_stream_summary(StreamName=stream_name)
    except ClientError as exc:
        if exc.response.get("Error", {}).get("Code") != "ResourceNotFoundException":
            raise
        try:
            client.create_stream(StreamName=stream_name, ShardCount=shard_count)
        except ClientError as create_exc:
            if create_exc.response.get("Error", {}).get("Code") != "ResourceInUseException":
                raise

    deadline = time.monotonic() + 60
    while time.monotonic() < deadline:
        try:
            summary = client.describe_stream_summary(StreamName=stream_name)[
                "StreamDescriptionSummary"
            ]
            if summary["StreamStatus"] == "ACTIVE":
                return
        except ClientError:
            pass
        time.sleep(1)
    raise TimeoutError(
        f"Kinesis stream {stream_name!r} did not become ACTIVE within 60 seconds"
    )


def publish_events(count: int, seed: int = 42) -> int:
    if count < 1:
        raise ValueError("count must be at least 1")
    settings = get_settings()
    client = create_client()
    ensure_stream(client, settings.stream_name, settings.shard_count)
    published = 0

    for index in range(count):
        event = build_event(index, seed)
        response = client.put_record(
            StreamName=settings.stream_name,
            Data=event.model_dump_json(),
            PartitionKey=event.hotel_id,
        )
        published += 1
        logger.info(
            "published event_id=%s event_type=%s hotel_id=%s shard_id=%s sequence_number=%s",
            event.event_id,
            event.event_type,
            event.hotel_id,
            response["ShardId"],
            response["SequenceNumber"],
        )

    logger.info(
        "publish complete: %d/%d events sent to %s",
        published,
        count,
        settings.stream_name,
    )
    return published


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--count", type=int, default=20, help="Number of synthetic events to publish"
    )
    parser.add_argument("--seed", type=int, default=42, help="Seed for reproducible test events")
    args = parser.parse_args()
    publish_events(args.count, args.seed)


if __name__ == "__main__":
    main()
