# Kafka local runbook

## Start the local broker

```powershell
Copy-Item .env.example .env
docker compose up -d kafka
docker compose ps
docker compose logs kafka
```

This is a single-node KRaft development broker, without authentication. It is not a production topology.

Create the three topics before running the producer or consumers:

```powershell
docker compose exec kafka kafka-topics --bootstrap-server kafka:29092 --create --if-not-exists --topic buenavista.reservations.v1 --partitions 6 --replication-factor 1
docker compose exec kafka kafka-topics --bootstrap-server kafka:29092 --create --if-not-exists --topic buenavista.reservations.retry.v1 --partitions 6 --replication-factor 1
docker compose exec kafka kafka-topics --bootstrap-server kafka:29092 --create --if-not-exists --topic buenavista.reservations.dlq.v1 --partitions 6 --replication-factor 1
```

## Install and start the source

```powershell
py -3.12 -m venv .venv
.\.venv\Scripts\Activate.ps1
python -m pip install -e ".[dev]"
python -m src.producer --count 20 --seed 42
```

The simulator produces schema v1.1 events to `buenavista.reservations.v1`, keyed by reservation ID.

## Run the independent consumer groups

Apply `sql/streaming_schema.sql` to a disposable PostgreSQL database before starting the OLTP projector. Set `DATABASE_URL` via environment configuration; use the approved Neon connection only after the team authorizes the additive migration. The Bronze consumer writes under `.data/bronze` when `BRONZE_S3_BUCKET` is empty; for S3, provide the approved bucket and AWS credential chain.

```powershell
python -m src.consumer
```

In another terminal:

```powershell
python -m src.bronze_consumer
```

Each group consumes the main topic independently. Neon offset commits follow a successful database transaction. Bronze batches up to 500 records or waits up to 30 seconds, groups raw records by ingest date and hotel, writes newline-delimited objects, then commits the highest processed offset per partition. Replays are expected and handled using event IDs or Kafka coordinates; Silver deduplicates by event ID and Kafka coordinates.

## Continuous demonstration

```powershell
python -m src.producer --continuous --duration 75
```

Defaults: 5 events per second with a 15 events per second peak for 10 seconds every 60 seconds. These are simulator settings.

## Downstream path

Airflow scheduled batch jobs read S3 Bronze and build Silver (validation, deduplication, pseudonymization) and Gold (revenue facts/dimensions) for Power BI. This repository defines the Bronze handoff; deploy the Airflow transforms in the data-platform repository.

## Stop local services

```powershell
docker compose down
```

`docker compose down -v` removes local Kafka state. Production Kafka security, MSK settings, partition/retention sizing, AWS IAM, S3 lifecycle, batching, monitoring, and recovery validation need deployment review.
