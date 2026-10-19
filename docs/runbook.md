# Kafka local runbook

## Start local Kafka

```powershell
Copy-Item .env.example .env
docker compose up -d kafka
docker compose ps
docker compose logs kafka
```

The single-node KRaft broker is for local development and demonstrations only. It has no authentication and is not a production topology. The host bootstrap address is `localhost:9092`.

## Install

```powershell
py -3.12 -m venv .venv
.\.venv\Scripts\Activate.ps1
python -m pip install -e ".[dev]"
```

## Publish a batch

```powershell
python -m src.producer --count 20 --seed 42
```

The producer sends v1.1 synthetic events to `buenavista.reservations.v1`, keyed by reservation ID. The topic has six local partitions. Broker-side topic creation is disabled, so create topics before first publish:

```powershell
docker compose exec kafka kafka-topics --bootstrap-server kafka:29092 --create --if-not-exists --topic buenavista.reservations.v1 --partitions 6 --replication-factor 1
docker compose exec kafka kafka-topics --bootstrap-server kafka:29092 --create --if-not-exists --topic buenavista.reservations.dlq.v1 --partitions 6 --replication-factor 1
```

## Configure sinks and consume

For local-only sink demonstration set `DATABASE_URL` to a disposable PostgreSQL database, apply `sql/streaming_schema.sql`, and leave `BRONZE_S3_BUCKET` empty to write under `.data/bronze`. To use the approved Neon and S3 destinations, set their credentials via environment/AWS credential chain. Never commit secrets. The consumer does not create or alter tables automatically.

```powershell
python -m src.consumer
```

Offset commits happen after both sinks succeed. If either sink fails, restart/retry replays the event; deterministic event IDs make the writes idempotent. Invalid records are copied to the DLQ.

## Continuous stream

```powershell
python -m src.producer --continuous --duration 75
```

Defaults are 5 events/second, with 15 events/second for the first 10 seconds of each 60-second cycle. These are simulator settings, not measured production traffic.

## Inspect topics

```powershell
docker compose exec kafka kafka-topics --bootstrap-server kafka:29092 --list
docker compose exec kafka kafka-console-consumer --bootstrap-server kafka:29092 --topic buenavista.reservations.v1 --from-beginning --property print.key=true
```

## Stop local services

```powershell
docker compose down
```

Use `docker compose down -v` only when you intentionally want to delete local Kafka state. Production AWS/MSK, TLS/SASL, IAM, retention, partition count, replication, monitoring, and cost sizing require a separate deployment decision.
