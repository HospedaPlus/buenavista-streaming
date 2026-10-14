<div align="center">

# 🌊 Buenavista Streaming

### Real-time reservation event ingestion · HospedaPlus Data Platform

**A documented, testable streaming-source scaffold for the Buenavista project.**

[![CI](https://github.com/HospedaPlus/buenavista-streaming/actions/workflows/ci.yml/badge.svg)](https://github.com/HospedaPlus/buenavista-streaming/actions/workflows/ci.yml)
[![Python](https://img.shields.io/badge/Python-3.12%2B-3776AB?logo=python&logoColor=white)](https://www.python.org/)
[![AWS Kinesis](https://img.shields.io/badge/Streaming-Amazon%20Kinesis-FF9900?logo=amazonaws&logoColor=white)](https://aws.amazon.com/kinesis/data-streams/)
[![Docker](https://img.shields.io/badge/Local%20environment-Docker-2496ED?logo=docker&logoColor=white)](https://www.docker.com/)

[🚀 Quick start](#-quick-start) · [🧩 Architecture](#-how-it-works) · [📦 Event contract](#-event-contract) · [🧪 Testing](#-quality-checks) · [📚 Documentation](#-documentation)

</div>

---

## 🎯 Project purpose

Buenavista Streaming provides a **synthetic F2 channel-manager reservation event source** for developing and validating the HospedaPlus streaming-ingestion workflow.

It generates structured reservation lifecycle events, validates them, and publishes them to an Amazon Kinesis Data Stream. Local development uses LocalStack, so the producer can be tested without creating AWS resources.

> [!IMPORTANT]
> **Development scaffold — not a production deployment.** The events are synthetic and do not come from a live channel manager. Kinesis is the proposed transport and must be confirmed against the team's approved architecture before AWS deployment.

## ✨ What is included?

| Capability | Description |
|---|---|
| 🏨 Synthetic event source | Generates repeatable reservation lifecycle events |
| ✅ Schema validation | Pydantic validates required fields and basic data rules |
| 📡 Streaming producer | Publishes JSON events through the Kinesis API |
| 🧪 Local testing | Uses LocalStack to emulate Kinesis locally |
| 🔐 Privacy-aware payload | Excludes direct guest identifiers such as names, email addresses, and phone numbers |
| 🧰 Automated quality checks | Ruff linting, formatting checks, and pytest in GitHub Actions |
| 📖 Operational docs | Event contract and local runbook |

## 🧩 How it works

\`\`\`mermaid
flowchart TD
    A["🏨 Synthetic F2 event generator"] --> B["✅ Pydantic validation"]
    B --> C["📦 JSON event"]
    C --> D["📡 Kinesis Data Stream"]
    D -. "Next implementation stage" .-> E["⚙️ Downstream consumer"]
    E -.-> F["🗄️ Approved storage / analytics"]
    L["🧪 LocalStack"] --- D
\`\`\`

**Current implementation:** event generation, validation, and publishing to Kinesis. A downstream consumer, sink persistence, replay, and end-to-end delivery verification are not included yet.

### ⚠️ Architecture boundary

Airflow's Celery/RabbitMQ broker is used for task orchestration in the wider project. It is **not automatically the business-event stream**. Confirm the approved streaming technology and integration points before deploying this source.

## 🚀 Quick start

### Prerequisites

- [Python 3.12+](https://www.python.org/downloads/)
- [Docker Desktop](https://www.docker.com/products/docker-desktop/) with Docker Compose v2
- Git
- PowerShell on Windows, or a terminal on macOS/Linux

### 1. Clone the repository

    git clone https://github.com/HospedaPlus/buenavista-streaming.git
    cd buenavista-streaming

If you already cloned it, update your local copy instead:

    git pull origin main

### 2. Create and activate a virtual environment

**Windows PowerShell**

    py -3.12 -m venv .venv
    .\.venv\Scripts\Activate.ps1

**macOS / Linux**

    python3.12 -m venv .venv
    source .venv/bin/activate

### 3. Install dependencies

    python -m pip install --upgrade pip
    python -m pip install -e ".[dev]"

### 4. Configure local settings

**Windows PowerShell**

    Copy-Item .env.example .env

**macOS / Linux**

    cp .env.example .env

The example uses dummy local credentials and the LocalStack endpoint at http://localhost:4566. Never commit real credentials or secrets.

### 5. Start LocalStack

    docker compose up -d localstack
    docker compose ps

LocalStack should be running before you start the producer. If it is unhealthy, inspect the logs:

    docker compose logs localstack

### 6. Run the quality checks

    ruff check .
    ruff format --check .
    pytest

### 7. Publish 20 synthetic events

    python -m src.producer --count 20 --seed 42

- **--count 20** asks the producer to publish 20 records.
- **--seed 42** makes generated values repeatable for debugging and tests.

Look for a final log message confirming **20/20** events were published. This validates producer-to-stream publication only; it does not prove downstream persistence.

### 8. Inspect the local stream (optional)

If the AWS CLI is installed:

    aws --endpoint-url=http://localhost:4566 --region us-east-1 kinesis list-streams
    aws --endpoint-url=http://localhost:4566 --region us-east-1 kinesis describe-stream-summary --stream-name buenavista-reservation-events

A stream listed as ACTIVE confirms the stream exists. It does not by itself verify that a downstream consumer processed every record.

## 📦 Event contract

Every event uses a versioned envelope with these fields:

| Field | Purpose |
|---|---|
| event_id | Stable event identifier for the same seed and event index |
| schema_version | Contract version, currently 1.0 |
| source | f2-channel-manager |
| event_type | Reservation lifecycle operation |
| occurred_at | Timezone-aware timestamp normalized to UTC |
| hotel_id | Identifier from the shared 28-hotel catalog |
| reservation_id | Synthetic reservation reference |
| data | Minimal reservation facts |

### Supported event types

- 🟢 **reservation.created**
- 🟡 **reservation.updated**
- 🔴 **reservation.cancelled**

The payload includes check-in/check-out dates, room type, guest count, total amount, currency, and status. It deliberately excludes guest names, email addresses, phone numbers, and payment details.

Consumers should validate the schema version, deduplicate using event_id, and use idempotent writes. Do not claim exactly-once processing: a successful Kinesis publish acknowledgement does not guarantee downstream persistence.

## 🧪 Quality checks

Run the following from the repository root:

| Command | What it checks |
|---|---|
| ruff check . | Linting and common code issues |
| ruff format --check . | Consistent Python formatting |
| pytest | Automated unit tests |
| python -m src.producer --count 20 --seed 42 | Local producer smoke test, when LocalStack is running |

GitHub Actions runs linting, formatting, and tests on pushes to main / develop and on pull requests. Check the [Actions tab](https://github.com/HospedaPlus/buenavista-streaming/actions) for the latest result.

## 🔐 Configuration and security

Configuration is loaded from environment variables; see [.env.example](.env.example).

- 🛡️ Use IAM roles or short-lived credentials for AWS.
- 🚫 Never commit .env, access keys, tokens, or production secrets.
- 🧍 Do not add real guest personally identifiable information to synthetic events.
- 🔎 Grant only the permissions needed by the runtime identity.
- 🧪 Use LocalStack for local development and tests.

## 📚 Documentation

- [📘 Event contract](docs/event-contract.md)
- [🧯 Local runbook and troubleshooting](docs/runbook.md)
- [🌐 Docusaurus streaming-ingestion page](https://github.com/HospedaPlus/docusaurus/blob/main/docs/streaming-ingestion.md)
- [⚙️ GitHub Actions](https://github.com/HospedaPlus/buenavista-streaming/actions)

## 🗺️ Delivery checklist

- [x] Repository and initial source structure created
- [x] Versioned event envelope and synthetic generator
- [x] Local Kinesis-compatible development environment
- [x] CI checks configured
- [ ] Confirm event contract with the data-platform owner
- [ ] Pass local smoke test against LocalStack
- [ ] Implement and test downstream consumer, retries, and checkpointing as required
- [ ] Verify idempotent downstream persistence and replay
- [ ] Approve AWS IAM, stream settings, deployment target, and architecture
- [ ] Complete end-to-end verification

## 👥 Project context

Built for the **HospedaPlus / Buenavista data platform** as a development source for the streaming-ingestion workstream.

<div align="center">

**🌊 Generate responsibly. Validate every event. Verify the whole pipeline.**

</div>
