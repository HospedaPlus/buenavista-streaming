"""Environment-based settings for the F2 Kafka pipeline."""
import os
from dataclasses import dataclass
from typing import Any

from dotenv import load_dotenv

load_dotenv()


@dataclass(frozen=True)
class Settings:
    bootstrap_servers: str = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
    security_protocol: str = os.getenv("KAFKA_SECURITY_PROTOCOL", "PLAINTEXT")
    auth_mode: str = os.getenv("KAFKA_AUTH_MODE", "none").lower()
    aws_region: str = os.getenv("AWS_REGION", "")
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
    bronze_batch_timeout_seconds: int = int(os.getenv("BRONZE_BATCH_TIMEOUT_SECONDS", "30"))

    def __post_init__(self) -> None:
        topics = (self.topic, self.retry_topic, self.dlq_topic)
        if any(not topic.strip() for topic in topics) or len(set(topics)) != 3:
            raise ValueError("Kafka main, retry, and DLQ topic names must be non-empty and distinct")
        if self.max_retries < 0 or self.retry_delay_seconds < 0:
            raise ValueError("Kafka retry values cannot be negative")
        if self.bronze_batch_size < 1 or self.bronze_batch_timeout_seconds < 1:
            raise ValueError("Bronze batch size and timeout must be positive")
        if not self.bootstrap_servers.strip():
            raise ValueError("KAFKA_BOOTSTRAP_SERVERS cannot be empty")
        if self.auth_mode not in {"none", "msk_iam", "sasl"}:
            raise ValueError("KAFKA_AUTH_MODE must be none, msk_iam, or sasl")
        if self.auth_mode == "msk_iam":
            if not self.aws_region.strip():
                raise ValueError("AWS_REGION is required when KAFKA_AUTH_MODE=msk_iam")
            if self.security_protocol != "SASL_SSL":
                raise ValueError("MSK IAM requires KAFKA_SECURITY_PROTOCOL=SASL_SSL")
        if self.auth_mode == "sasl":
            if not all((self.sasl_mechanism, self.sasl_username, self.sasl_password)):
                raise ValueError("SASL mechanism, username, and password must all be set")
            if self.security_protocol != "SASL_SSL":
                raise ValueError("SASL authentication requires KAFKA_SECURITY_PROTOCOL=SASL_SSL")

    def kafka_security_config(self) -> dict[str, Any]:
        """Return shared Kafka TLS/SASL settings for the producer and consumers."""
        config: dict[str, Any] = {"security.protocol": self.security_protocol}
        if self.auth_mode == "msk_iam":
            from aws_msk_iam_sasl_signer import MSKAuthTokenProvider

            def oauth_cb(_oauth_config: str) -> tuple[str, float]:
                token, expiry_ms = MSKAuthTokenProvider.generate_auth_token(self.aws_region)
                # confluent-kafka expects OAuth expiry in seconds; AWS returns milliseconds.
                return token, expiry_ms / 1000

            config.update({
                "security.protocol": "SASL_SSL",
                "sasl.mechanism": "OAUTHBEARER",
                "oauth_cb": oauth_cb,
            })
        elif self.auth_mode == "sasl":
            config.update({
                "security.protocol": "SASL_SSL",
                "sasl.mechanism": self.sasl_mechanism,
                "sasl.username": self.sasl_username,
                "sasl.password": self.sasl_password,
            })
        if self.ssl_ca_location:
            config["ssl.ca.location"] = self.ssl_ca_location
        return config


def get_settings() -> Settings:
    return Settings()
