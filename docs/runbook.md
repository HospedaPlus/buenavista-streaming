# Local runbook

## Start RabbitMQ

```powershell
Copy-Item .env.example .env
docker compose up -d rabbitmq
docker compose ps
docker compose logs rabbitmq
```

RabbitMQ AMQP port: `5672`. Management UI: `http://localhost:15672`. Local defaults are `guest/guest`; use them only for local development.

## Install and validate

```powershell
py -3.12 -m venv .venv
.\.venv\Scripts\Activate.ps1
python -m pip install -e ".[dev]"
ruff check .
ruff format --check .
pytest
```

## Publish a controlled batch

```powershell
python -m src.producer --count 20 --seed 42
```

Expected: the summary line reports `total=20` messages confirmed by RabbitMQ. Open the management UI, select Queues and Streams, and inspect `buenavista.reservation.events`; the ready-message count should reflect messages waiting for a consumer.

## Run the continuous stream

```powershell
python -m src.producer --continuous --duration 75
```

Expected: one `throughput` line per second, 15 events per second during the first 10 seconds of each minute and 5 otherwise, then a summary with `peak=15 ev/s`. Run `python -m src.consumer` in another terminal to drain the queue; in the management UI the exchange `buenavista.reservations` shows the publish rate.

## Troubleshooting

- **Connection refused:** run `docker compose ps` and `docker compose logs rabbitmq`; check port 5672.
- **Authentication failure:** confirm local `RABBITMQ_USER` and `RABBITMQ_PASSWORD` match the container settings. If changing defaults after the RabbitMQ volume has already been initialized, recreating the container alone may not reset credentials.
- **Queue has zero ready messages:** confirm the publish logs; a consumer may already have received messages.
- **Duplicate processing:** use `event_id` as the downstream idempotency key.
- **Messages repeatedly fail:** define bounded retries and a dead-letter queue before production.

## Stop local services

```powershell
docker compose down
```

Delete the local RabbitMQ volume as well (this removes local broker state):

```powershell
docker compose down -v
```

Do not expose port 5672 or the management UI publicly. Use a secured, approved deployment configuration outside local development.
