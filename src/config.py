"""Load local RabbitMQ settings from environment or .env."""

import os
from dataclasses import dataclass

from dotenv import load_dotenv

load_dotenv()


@dataclass(frozen=True)
class Settings:
    """Connection and queue settings for the RabbitMQ producer."""

    host: str = os.getenv("RABBITMQ_HOST", "localhost")
    port: int = int(os.getenv("RABBITMQ_PORT", "5672"))
    username: str = os.getenv("RABBITMQ_USER", "guest")
    password: str = os.getenv("RABBITMQ_PASSWORD", "guest")
    virtual_host: str = os.getenv("RABBITMQ_VHOST", "/")
    queue_name: str = os.getenv("RABBITMQ_QUEUE", "buenavista.reservation.events")


def get_settings() -> Settings:
    """Return current settings as an immutable object."""
    return Settings()
