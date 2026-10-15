# Buenavista Streaming Ingestion

Synthetic streaming source for HospedaPlus/Buenavista. This repository generates schema-validated F2 channel-manager reservation events and publishes them to a durable RabbitMQ queue.

> **Development status:** synthetic events only. Local publication must be tested before claiming the source is ready. RabbitMQ is selected to align with the project's cost-conscious architecture; no managed Kinesis stream is required for this local implementation.

## Architecture

```text
Synthetic F2 event generator
        |
        v
Pydantic event validation
        |
        v
RabbitMQ durable queue
        |
        v
Consumer (next stage)
        |
        v
Approved data destination
```

RabbitMQ is already part of the wider project architecture for asynchronous workloads. The team should confirm the queue and consumer integration points against the final architecture diagram. Airflow/Celery task messages and business reservation events should use clearly separated queues and routing conventions.

## Event types

- `reservation.created`
- `reservation.updated`
- `reservation.cancelled`

Events include a versioned envelope, UTC occurrence time, hotel ID from the shared 28-hotel catalog, synthetic reservation ID, and minimal reservation facts. Direct guest identifiers and payment details are excluded.

## Requirements

- Python 3.12+
- Docker Desktop with Docker Compose v2
- Git

## Quick start — Windows PowerShell

```powershell
git clone https://github.com/HospedaPlus/buenavista-streaming.git
cd buenavista-streaming
py -3.12 -m venv .venv
.\.venv\Scripts\Activate.ps1
python -m pip install --upgrade pip
python -m pip install -e ".[dev]"
Copy-Item .env.example .env
docker compose up -d rabbitmq
docker compose ps
ruff check .
ruff format --check .
pytest
python -m src.producer --count 20 --seed 42
```

macOS/Linux: use `python3.12 -m venv .venv`, `source .venv/bin/activate`, and `cp .env.example .env`.

RabbitMQ AMQP is available at `localhost:5672`; the management UI is at `http://localhost:15672`. Local development defaults use `guest/guest`; do not expose this configuration to a network or reuse it in production.

## Configuration

See `.env.example`:
- `RABBITMQ_HOST`
- `RABBITMQ_PORT`
- `RABBITMQ_USER`
- `RABBITMQ_PASSWORD`
- `RABBITMQ_VHOST`
- `RABBITMQ_QUEUE`

The producer declares the queue as durable, sends persistent messages, and enables publisher confirms. A broker confirmation means RabbitMQ accepted the message; it does not prove downstream processing or database persistence.

## Quality checks

```powershell
ruff check .
ruff format --check .
pytest
python -m src.producer --count 20 --seed 42
```

## Acceptance checklist

- [ ] Confirm the event contract with the data-platform owner.
- [ ] Pass unit tests and Ruff checks.
- [ ] Verify RabbitMQ health and local publishing.
- [ ] Implement and test a consumer with acknowledgement, retry, and dead-letter strategy as required.
- [ ] Verify idempotent persistence to the approved destination.
- [ ] Confirm queue naming and separation from Airflow/Celery queues.
- [ ] Confirm production credentials, network exposure, monitoring, and deployment ownership.
- [ ] Publish final docs in Docusaurus.

This is not a production deployment. No live reservation system is connected.
