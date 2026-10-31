# Kafka local runbook

## Start Kafka

```powershell
Copy-Item .env.example .env
docker compose -f docker-compose.kafka.yml up -d
docker compose -f docker-compose.kafka.yml ps
docker compose -f docker-compose.kafka.yml logs --tail 100
```

## Install and validate

```powershell
py -3.12 -m venv .venv
.\.venv\Scripts\Activate.ps1
python -m pip install -e ".[dev]"
ruff check .
ruff format --check .
pytest
```

## Create the topic and publish test events

```powershell
docker exec buenavista-streaming-kafka /opt/kafka/bin/kafka-topics.sh --bootstrap-server localhost:9092 --create --if-not-exists --topic reservation.events.v1_3 --partitions 3 --replication-factor 1
docker exec buenavista-streaming-kafka /opt/kafka/bin/kafka-topics.sh --bootstrap-server localhost:9092 --create --if-not-exists --topic reservation.events.dlq --partitions 1 --replication-factor 1
python -m src.kafka_producer --count 20 --seed 42
# continuous stream with peaks (Ctrl+C to stop)
python -m src.kafka_producer --continuous --duration 120
```

The local Kafka broker uses a single node and replication factor one; this is for development only.

## Run the validation consumer

In another terminal:

```powershell
python -m src.kafka_consumer
```

The validation consumer checks the schema and logs safe metadata. It does not write anything.

## Run the Neon ledger consumer

Only after the additive migration has been reviewed and applied through the approved database process, set `NEON_DATABASE_URL` in your untracked local `.env` file and run:

```powershell
python -m src.neon_consumer
```

Live Neon persistence must be verified before treating the sink as operational.

## Troubleshooting

- **Connection refused:** inspect `docker compose -f docker-compose.kafka.yml ps` and the broker logs; confirm port 9092.
- **Topic not found:** create `reservation.events.v1_3` with the command above.
- **Duplicate events:** use `event_id` as the idempotency key and `reservation_version` for ordering.
- **Database failure:** do not commit the Kafka offset until the destination transaction succeeds.
- **Invalid event:** the validation consumer stops on it; the Neon consumer copies it to `reservation.events.dlq` and continues. Create that topic like the main one.

## Stop Kafka

```powershell
docker compose -f docker-compose.kafka.yml down
```

This stops local containers but preserves broker data unless volumes are explicitly removed. Do not expose development broker ports to untrusted networks.
