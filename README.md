# Buenavista Streaming Ingestion

Synthetic streaming source for HospedaPlus/Buenavista. This repository simulates the F2 source (channel-manager events): it generates schema-validated JSON reservation events, continuously and with periodic peaks of at least 10 events per second, and publishes them to RabbitMQ through a durable topic exchange and queue.

> **Development status:** synthetic events only. Local publication must be tested before claiming the source is ready. RabbitMQ is selected to align with the project's cost-conscious architecture; no managed Kinesis stream is required for this local implementation.

## Architecture

```text
Synthetic F2 event generator
        |
        v
Pydantic event validation
        |
        v
RabbitMQ topic exchange buenavista.reservations (routing key = event type)
        |  binding reservation.#
        v
RabbitMQ durable queue buenavista.reservation.events
        |
        v
Consumer (next stage)
        |
        v
Approved data destination
```

RabbitMQ is already part of the wider project architecture for asynchronous workloads. The team should confirm the queue and consumer integration points against the final architecture diagram. Airflow/Celery task messages and business reservation events should use clearly separated queues and routing conventions.

## Event types

| Event type | Meaning |
|---|---|
| `reservation.created` | Reservation created |
| `reservation.updated` | Reservation modified (dates, room type or guests); a reservation can have several |
| `reservation.cancelled` | Reservation cancelled; about 30% of cancellations are late (< 24 h before arrival) |
| `reservation.no_show` | Guest did not arrive |
| `reservation.checked_in` | Guest checked in |
| `reservation.checked_out` | Guest checked out |

Each synthetic reservation follows a full lifecycle: creation, zero to three modifications, then one outcome (cancellation about 37% as in the business context, no-show about 5%, otherwise check-in and check-out). Reservations of all 28 hotels are interleaved in simulated chronological order. By default 5% of events are delivered out of order (for example a modification before its creation); `reservation_version` lets consumers put them back in order.

Events include a versioned envelope, simulated UTC occurrence time, hotel ID from the shared 28-hotel catalog, synthetic reservation ID, sales channel, refundable flag, and minimal reservation facts. Direct guest identifiers and payment details are excluded. See [docs/event-contract.md](docs/event-contract.md).

## Running the stream

```powershell
# Continuous stream: 5 ev/s base, 15 ev/s peak for 10 s every 60 s, until Ctrl+C
python -m src.producer --continuous

# Same, limited to 75 seconds (covers two peaks)
python -m src.producer --continuous --duration 75

# Batch mode (original behaviour): publish 20 events as fast as possible and stop
python -m src.producer --count 20 --seed 42

# In another terminal: consume and validate
python -m src.consumer
```

Continuous mode logs the confirmed events of every second and ends with a summary such as `total=575 elapsed=75.1s average=7.7 ev/s peak=15 ev/s`. Options: `--rate`, `--peak-rate`, `--peak-every` (0 disables peaks), `--peak-duration`, `--duration`, `--count`, `--seed`, `--out-of-order-rate`, `--verbose` (log every event).

Measured locally against the Compose RabbitMQ with publisher confirms: the default profile sustained exactly 15 ev/s during peaks and 5 ev/s otherwise, and the producer reached about 950 ev/s in batch mode, so the ≥ 10 ev/s peak has wide headroom.

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
- `RABBITMQ_EXCHANGE`
- `RABBITMQ_MAX_RETRIES`, `RABBITMQ_RETRY_DELAY_MS`, `RABBITMQ_DLQ`

The producer and the consumer both declare the durable topic exchange, the durable queue and their binding (idempotent). The producer sends persistent messages, and enables publisher confirms. A broker confirmation means RabbitMQ accepted the message; it does not prove downstream processing or database persistence.

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
- [x] Implement and test a consumer with acknowledgement, retry, and dead-letter strategy.
- [x] Emit all six F2 event types continuously with peaks of at least 10 ev/s, including out-of-order events.
- [ ] Verify idempotent persistence to the approved destination.
- [ ] Confirm queue naming and separation from Airflow/Celery queues.
- [ ] Confirm production credentials, network exposure, monitoring, and deployment ownership.
- [ ] Publish final docs in Docusaurus.

This is not a production deployment. No live reservation system is connected.
