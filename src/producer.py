"""Publish synthetic F2 reservation events to Amazon Kinesis Data Streams.

Use LocalStack for local validation. A successful publish confirms that Kinesis
accepted a record; it does not prove downstream persistence or exactly-once processing.
"""

import argparse
import logging
import time

import boto3
from botocore.exceptions import ClientError

from src.config import get_settings
from src.events import build_event

# Include event identifiers and Kinesis sequence information in operational logs.
logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
logger = logging.getLogger(__name__)


def create_client():
    """Build a Kinesis client from the current environment configuration."""
    settings = get_settings()
    kwargs = {
        "service_name": "kinesis",
        "region_name": settings.region,
        "aws_access_key_id": settings.access_key_id,
        "aws_secret_access_key": settings.secret_access_key,
    }

    # boto3 uses this override for LocalStack; omit it when targeting real AWS.
    if settings.endpoint_url:
        kwargs["endpoint_url"] = settings.endpoint_url
    return boto3.client(**kwargs)


def ensure_stream(client, stream_name: str, shard_count: int) -> None:
    """Create the stream when absent and wait until Kinesis reports it ACTIVE."""
    try:
        client.describe_stream_summary(StreamName=stream_name)
    except ClientError as exc:
        # Only a missing stream is recoverable here; re-raise other AWS/API errors.
        if exc.response.get("Error", {}).get("Code") != "ResourceNotFoundException":
            raise
        try:
            client.create_stream(StreamName=stream_name, ShardCount=shard_count)
        except ClientError as create_exc:
            # Another process may have created the stream between describe and create.
            if create_exc.response.get("Error", {}).get("Code") != "ResourceInUseException":
                raise

    # Stream creation is asynchronous, so poll its state with a bounded wait.
    deadline = time.monotonic() + 60
    while time.monotonic() < deadline:
        try:
            summary = client.describe_stream_summary(StreamName=stream_name)[
                "StreamDescriptionSummary"
            ]
            if summary["StreamStatus"] == "ACTIVE":
                return
        except ClientError:
            # Retry transient state errors until the timeout; final errors surface below.
            pass
        time.sleep(1)

    raise TimeoutError(f"Kinesis stream {stream_name!r} did not become ACTIVE within 60 seconds")


def publish_events(count: int, seed: int = 42) -> int:
    """Generate and publish count records; return the number acknowledged by Kinesis."""
    if count < 1:
        raise ValueError("count must be at least 1")

    settings = get_settings()
    client = create_client()
    ensure_stream(client, settings.stream_name, settings.shard_count)
    published = 0

    for index in range(count):
        # Validate one event and serialize it as JSON before sending it to Kinesis.
        event = build_event(index, seed)
        response = client.put_record(
            StreamName=settings.stream_name,
            Data=event.model_dump_json(),
            # Group records by hotel so records for the same hotel share a partition key.
            PartitionKey=event.hotel_id,
        )
        published += 1

        # Record the service acknowledgement details for debugging and auditability.
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
    """Parse command-line options and start a controlled publish batch."""
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--count", type=int, default=20, help="Number of synthetic events to publish"
    )
    parser.add_argument("--seed", type=int, default=42, help="Seed for reproducible test events")
    args = parser.parse_args()
    publish_events(args.count, args.seed)


if __name__ == "__main__":
    # Run the CLI only when this module is executed directly.
    main()
