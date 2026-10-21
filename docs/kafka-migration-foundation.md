# Kafka streaming foundation

## Goal

Buenavista Streaming uses Apache Kafka as its only supported message broker. Events originate from the canonical HospedaPlus simulated data in [`HospedaPlus/db_simulated`](https://github.com/HospedaPlus/db_simulated), with future event sources provided by approved project APIs. Independent consumer groups are intended to deliver applicable events to Neon PostgreSQL and the Amazon S3 Bronze layer.

## Target architecture

```text
db_simulated seed ─┐
                   ├─> producer + Pydantic validation ─> Apache Kafka
Project APIs ──────┘                                  │
                                      topic: reservation.events.v1_2
                                                     │
                               ┌─────────────────────┴────────────────────┐
                               v                                          v
                      Neon consumer                              S3 Bronze consumer
                               │                                          │
                               v                                          v
                     Neon PostgreSQL                              Amazon S3 / bronze
```

## Design rules

1. Keep `db_simulated` as the canonical source for hotel, room-type, channel, and other seeded catalog values.
2. Start with `reservation.events.v1_2`. Add other topics only when their event contracts and producers are ready.
3. Keep Neon and S3 as independent consumer groups so a failure in one sink does not directly stop the other.
4. Design consumers for at-least-once delivery, idempotent writes, bounded retries, and recoverable failures. Do not claim exactly-once writes across multiple systems.
5. Commit consumer offsets only after the corresponding sink write succeeds.
6. Do not blindly write all event types to the operational `reservations` table. Map event types to the existing database schema and business rules.
7. Keep the local single-node broker configuration limited to development and demonstrations.

## Local Kafka broker

`docker-compose.kafka.yml` provides a single-node Kafka KRaft broker for local development.

Start Kafka:

```powershell
docker compose -f docker-compose.kafka.yml up -d
docker compose -f docker-compose.kafka.yml ps
```

Create the initial topic:

```powershell
docker exec buenavista-streaming-kafka /opt/kafka/bin/kafka-topics.sh --bootstrap-server localhost:9092 --create --if-not-exists --topic reservation.events.v1_2 --partitions 3 --replication-factor 1
```

Inspect topics:

```powershell
docker exec buenavista-streaming-kafka /opt/kafka/bin/kafka-topics.sh --bootstrap-server localhost:9092 --list
```

Stop Kafka:

```powershell
docker compose -f docker-compose.kafka.yml down
```

The named `kafka_data` volume is retained by this command. Do not add `-v` unless deleting local Kafka data is explicitly intended.

## Implementation status

- [x] Local Kafka KRaft broker configuration.
- [x] Versioned reservation event contract.
- [x] Kafka producer and schema-validation consumer.
- [x] Automated producer and consumer unit tests.
- [x] Isolated Neon event-ledger sink and consumer.
- [ ] Kafka broker integration tests from a clean environment.
- [ ] Review and apply the Neon event-ledger migration through the approved process.
- [ ] Verify live Neon persistence with integration tests.
- [ ] Implement and test S3 Bronze persistence.
- [ ] Implement bounded retries and a dead-letter workflow.
- [ ] Validate catalog compatibility, replay, idempotency, and offset behavior end to end.

## Limitations

The local broker uses one node and replication factor one, so it is not suitable for production high availability. Neon integration must not be considered operational until the migration and connection are verified. S3 persistence, production failure recovery, and cloud deployment remain pending.
