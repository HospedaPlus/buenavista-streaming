"""Load RabbitMQ connection, queue, and retry settings."""

import os
from dataclasses import dataclass

from dotenv import load_dotenv

# Load local environment variables for development.
# Existing system environment variables remain available as well.
load_dotenv()


@dataclass(frozen=True)
class Settings:
    """Store immutable settings for RabbitMQ streaming ingestion."""

    # RabbitMQ connection settings shared by the producer and consumer.
    host: str = os.getenv("RABBITMQ_HOST", "localhost")
    port: int = int(os.getenv("RABBITMQ_PORT", "5672"))
    username: str = os.getenv("RABBITMQ_USER", "guest")
    password: str = os.getenv("RABBITMQ_PASSWORD", "guest")
    virtual_host: str = os.getenv("RABBITMQ_VHOST", "/")

    # Main queue where reservation events are initially published.
    queue_name: str = os.getenv(
        "RABBITMQ_QUEUE",
        "buenavista.reservation.events",
    )

    # Topic exchange that receives every reservation event. The routing key is
    # the event type (for example reservation.cancelled).
    exchange_name: str = os.getenv(
        "RABBITMQ_EXCHANGE",
        "buenavista.reservations",
    )

    # Maximum number of additional processing attempts for transient failures.
    # Zero disables retries; negative values are invalid.
    max_retries: int = int(os.getenv("RABBITMQ_MAX_RETRIES", "3"))

    # Delay before a failed message becomes available for another attempt.
    # RabbitMQ will enforce this delay through a retry queue and message TTL.
    retry_delay_ms: int = int(os.getenv("RABBITMQ_RETRY_DELAY_MS", "10000"))

    # Queue that retains invalid or repeatedly failing messages for inspection.
    dead_letter_queue: str = os.getenv(
        "RABBITMQ_DLQ",
        "buenavista.reservation.events.dlq",
    )

    def __post_init__(self) -> None:
        """Reject invalid retry settings before starting the application."""
        if self.max_retries < 0:
            raise ValueError("RABBITMQ_MAX_RETRIES cannot be negative")

        if self.retry_delay_ms < 0:
            raise ValueError("RABBITMQ_RETRY_DELAY_MS cannot be negative")

        if not self.queue_name.strip():
            raise ValueError("RABBITMQ_QUEUE cannot be empty")

        if not self.exchange_name.strip():
            raise ValueError("RABBITMQ_EXCHANGE cannot be empty")

        if not self.dead_letter_queue.strip():
            raise ValueError("RABBITMQ_DLQ cannot be empty")


def get_settings() -> Settings:
    """Return the current immutable application configuration."""
    return Settings()
