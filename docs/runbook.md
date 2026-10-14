# Local runbook

## Start the local stream emulator

```bash
cp .env.example .env
docker compose up -d localstack
docker compose ps
```

On Windows PowerShell, create and activate a virtual environment:

```powershell
py -3.12 -m venv .venv
.\.venv\Scripts\Activate.ps1
pip install -e ".[dev]"
```

On macOS/Linux use `python3.12 -m venv .venv` and `source .venv/bin/activate`.

## Validate and publish

```bash
ruff check .
ruff format --check .
pytest
python -m src.producer --count 20 --seed 42
```

Expected: tests pass and logs show 20 records published, including each record's shard ID and sequence number. This validates producer-to-stream publication only; it does not validate delivery to a downstream database or analytics table.

## Inspect the stream

If AWS CLI is installed, use LocalStack's endpoint:

```bash
aws --endpoint-url=http://localhost:4566 --region us-east-1 kinesis list-streams
aws --endpoint-url=http://localhost:4566 --region us-east-1 kinesis describe-stream-summary --stream-name buenavista-reservation-events
```

## Troubleshooting

- **Connection refused:** check `docker compose ps` and `docker compose logs localstack`; port 4566 must be available.
- **Stream does not become active:** inspect LocalStack logs and confirm Kinesis is included in `SERVICES`.
- **AWS auth errors:** local mode uses dummy credentials from `.env.example`; never copy those into an AWS deployment.
- **Wrong endpoint in AWS:** unset `AWS_ENDPOINT_URL` for real AWS so boto3 uses the standard AWS endpoint.
- **Duplicates downstream:** use `event_id` as an idempotency key; Kinesis producer acknowledgements do not imply exactly-once sink processing.

## Cleanup

```bash
docker compose down
```

To also delete persisted LocalStack state:

```bash
docker compose down -v
```
