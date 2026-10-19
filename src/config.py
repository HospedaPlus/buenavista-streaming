"""Environment-based settings for the F2 Kafka pipeline."""
import os
from dataclasses import dataclass
from dotenv import load_dotenv

load_dotenv()

@dataclass(frozen=True)
class Settings:
    bootstrap_servers: str = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
    topic: str = os.getenv("KAFKA_TOPIC", "buenavista.reservations.v1")
    dlq_topic: str = os.getenv("KAFKA_DLQ_TOPIC", "buenavista.reservations.dlq.v1")
    consumer_group: str = os.getenv("KAFKA_CONSUMER_GROUP", "buenavista-bronze-neon-v1")
    database_url: str = os.getenv("DATABASE_URL", "")
    bronze_bucket: str = os.getenv("BRONZE_S3_BUCKET", "")
    bronze_prefix: str = os.getenv("BRONZE_S3_PREFIX", "bronze/source=f2_channel_manager")
    bronze_local_dir: str = os.getenv("BRONZE_LOCAL_DIR", ".data/bronze")
    security_protocol: str = os.getenv("KAFKA_SECURITY_PROTOCOL", "PLAINTEXT")

    def __post_init__(self) -> None:
        if not self.bootstrap_servers.strip():
            raise ValueError("KAFKA_BOOTSTRAP_SERVERS cannot be empty")
        if not self.topic.strip() or not self.dlq_topic.strip():
            raise ValueError("Kafka topic names cannot be empty")
        if self.topic == self.dlq_topic:
            raise ValueError("KAFKA_TOPIC and KAFKA_DLQ_TOPIC must differ")

def get_settings() -> Settings:
    return Settings()
