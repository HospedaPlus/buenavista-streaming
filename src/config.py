"""Environment-based configuration for the streaming producer."""

import os
from dataclasses import dataclass

from dotenv import load_dotenv

load_dotenv()


@dataclass(frozen=True)
class Settings:
    region: str = os.getenv("AWS_REGION", "us-east-1")
    access_key_id: str = os.getenv("AWS_ACCESS_KEY_ID", "test")
    secret_access_key: str = os.getenv("AWS_SECRET_ACCESS_KEY", "test")
    endpoint_url: str | None = os.getenv("AWS_ENDPOINT_URL", "http://localhost:4566")
    stream_name: str = os.getenv("KINESIS_STREAM_NAME", "buenavista-reservation-events")
    shard_count: int = int(os.getenv("KINESIS_SHARD_COUNT", "1"))


def get_settings() -> Settings:
    """Return settings; AWS_ENDPOINT_URL should be unset when using real AWS."""
    return Settings()
