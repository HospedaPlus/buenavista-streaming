"""Environment-based settings for the F2 Kafka pipeline."""
import os
from dataclasses import dataclass

from dotenv import load_dotenv

load_dotenv()


@dataclass(frozen=True)
class Settings:
    bootstrap_servers: str = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
    security_protocol: str = os.getenv("KAFKA_SECURITY_PROTOCOL", "PLAINTEXT")
    sasl_mechanism: str = os.getenv("KAFKA_SASL_MECHANISM", "")
    sasl_username: str = os.getenv("KAFKA_SASL_USERNAME", "")
    sasl_password: str = os.getenv("KAFKA_SASL_PASSWORD", "")
    ssl_ca_location: str = os.getenv("KAFKA_SSL_CA_LOCATION", "")
    topic: str = os.getenv("KAFKA_TOPIC", "buenavista.reservations.v1")
    retry_topic: str = os.getenv("KAFKA_RETRY_TOPIC", "buenavista.reservations.retry.v1")
    dlq_topic: str = os.getenv("KAFKA_DLQ_TOPIC", "buenavista.reservations.dlq.v1")
    neon_consumer_group: str = os.getenv("KAFKA_NEON_GROUP", "buenavista-neon-projector-v1")
    bronze_consumer_group: str = os.getenv("KAFKA_BRONZE_GROUP", "buenavista-bronze-capture-v1")
    max_retries: int = int(os.getenv("KAFKA_MAX_RETRIES", "3"))
    retry_delay_seconds: int = int(os.getenv("KAFKA_RETRY_DELAY_SECONDS", "5"))
    database_url: str = os.getenv("DATABASE_URL", "")
    bronze_bucket: str = os.getenv("BRONZE_S3_BUCKET", "")
    bronze_prefix: str = os.getenv("BRONZE_S3_PREFIX", "bronze/source=f2_channel_manager")
    bronze_local_dir: str = os.getenv("BRONZE_LOCAL_DIR", ".data/bronze")
    bronze_batch_size: int = int(os.getenv("BRONZE_BATCH_SIZE", "500"))
    bronze_batch_timeout_seconds: int = int(
        os.getenv("BRONZE_BATCH_TIMEOUT_SECONDS", "30")
    )

    def __post_init__(self) -> None:
        topics = (self.topic, self.retry_topic, self.dlq_topic)
        if any(not topic.strip() for topic in topics) or len(set(topics)) != 3:
            raise ValueError(
                "Kafka main, retry, and DLQ topic names must be non-empty and distinct"
            )
        if self.max_retries < 0 or self.retry_delay_seconds < 0:
            raise ValueError("Kafka retry values cannot be negative")
        if self.bronze_batch_size < 1 or self.bronze_batch_timeout_seconds < 1:
            raise ValueError("Bronze batch size and timeout must be positive")
        if not self.bootstrap_servers.strip():
            raise ValueError("KAFKA_BOOTSTRAP_SERVERS cannot be empty")
        sasl_values = (self.sasl_mechanism, self.sasl_username, self.sasl_password)
        if any(sasl_values) and not all(sasl_values):
            raise ValueError("Kafka SASL mechanism, username, and password must be set together")
        if all(sasl_values) and self.security_protocol != "SASL_SSL":
            raise ValueError("Kafka SASL credentials require KAFKA_SECURITY_PROTOCOL=SASL_SSL")

    def kafka_security_config(self) -> dict[str, str]:
        """Return the shared librdkafka TLS/SASL settings for every client."""
        config = {"security.protocol": self.security_protocol}
        if self.sasl_mechanism:
            config.update({
                "sasl.mechanism": self.sasl_mechanism,
                "sasl.username": self.sasl_username,
                "sasl.password": self.sasl_password,
            })
        if self.ssl_ca_location:
            config["ssl.ca.location"] = self.ssl_ca_location
        return config


def get_settings() -> Settings:
    return Settings()
