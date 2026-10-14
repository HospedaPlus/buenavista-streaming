# Buenavista Streaming Ingestion

Streaming source simulator for the HospedaPlus/Buenavista data platform. This repository starts with the **F2 channel-manager reservation event stream** as a synthetic source; it does not connect to or scrape a real channel manager.

## Current scope

- Generate deterministic, schema-validated reservation lifecycle events.
- Publish events to an Amazon Kinesis Data Stream.
- Run locally against LocalStack for development and tests.
- Keep event payloads free of guest names, emails, phone numbers, and other direct identifiers.
- Document the source contract and operational checks.

> **Architecture note:** Kinesis is the proposed streaming transport for this source. Confirm it against the team's approved architecture before deploying to AWS. Airflow's Celery/RabbitMQ broker is not automatically the business-event stream.

## Event types

- `reservation.created`
- `reservation.updated`
- `reservation.cancelled`

Each event contains an event ID, schema version, source name, event type, UTC occurrence timestamp, hotel ID, reservation ID, and a minimal reservation payload. Hotel IDs must follow the shared catalog convention (for example, `HP-CO-001`).

## Quick start (local)

Requirements: Docker Compose v2 and Python 3.12+.

```bash
cp .env.example .env
docker compose up -d localstack
python -m venv .venv
# Windows PowerShell:
.venv\Scripts\Activate.ps1
# macOS/Linux:
# source .venv/bin/activate
pip install -e ".[dev]"
python -m src.producer --count 20
pytest
```

LocalStack exposes its AWS-compatible API at `http://localhost:4566`. The producer creates the configured stream if it does not exist.

## Configuration

See `.env.example`. Local development uses LocalStack and dummy local credentials. For AWS, use an IAM role or short-lived credentials; never commit AWS keys or production secrets.

## Run against AWS

1. Create a Kinesis Data Stream in the approved AWS account and region, with retention and capacity configured to the team's requirements.
2. Grant the runtime identity only the required permissions (at minimum `kinesis:DescribeStreamSummary`, `kinesis:CreateStream` if stream creation is permitted, and `kinesis:PutRecord`).
3. Set `AWS_REGION`, `KINESIS_STREAM_NAME`, and `AWS_ENDPOINT_URL` to the approved values; unset `AWS_ENDPOINT_URL` when using AWS.
4. Run `python -m src.producer --count 100` as a controlled smoke test and verify records/metrics in the stream.
5. Do not treat a successful publish as proof of downstream persistence. Consumer/sink integration, replay policy, monitoring, and deployment approval are separate acceptance items.

## Quality checks

```bash
ruff check .
ruff format --check .
pytest
```

## Acceptance checklist

- [ ] Event contract reviewed against the approved source requirements.
- [ ] Hotel IDs validated against the shared 28-hotel catalog.
- [ ] Local smoke test passes against LocalStack.
- [ ] Unit tests pass.
- [ ] AWS IAM, stream settings, and deployment target approved by the team.
- [ ] Docusaurus source documentation published.
- [ ] End-to-end downstream delivery and replay tested.

## Status

This is a development scaffold, not yet a production deployment. The generated events are synthetic and must not be presented as live reservations.
