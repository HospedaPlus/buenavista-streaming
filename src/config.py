"""Load local or AWS settings for the streaming producer.

Environment variables are read from the process environment and, when present,
the local .env file. LocalStack is the safe default for development.
"""

import os
from dataclasses import dataclass

from dotenv import load_dotenv

# Load developer-local values without overriding variables already set by the shell.
load_dotenv()


@dataclass(frozen=True)
class Settings:
    """Immutable configuration used to construct the Kinesis client."""

    # AWS region for the stream; LocalStack defaults to us-east-1.
    region: str = os.getenv("AWS_REGION", "us-east-1")

    # LocalStack accepts dummy credentials; AWS should use an approved identity.
    access_key_id: str = os.getenv("AWS_ACCESS_KEY_ID", "test")
    secret_access_key: str = os.getenv("AWS_SECRET_ACCESS_KEY", "test")

    # Keep the local endpoint for development; unset AWS_ENDPOINT_URL for real AWS.
    endpoint_url: str | None = os.getenv("AWS_ENDPOINT_URL", "http://localhost:4566")

    # Name and initial shard count of the Kinesis stream created by the producer.
    stream_name: str = os.getenv("KINESIS_STREAM_NAME", "buenavista-reservation-events")
    shard_count: int = int(os.getenv("KINESIS_SHARD_COUNT", "1"))


def get_settings() -> Settings:
    """Return the current configuration as a typed settings object."""
    return Settings()
