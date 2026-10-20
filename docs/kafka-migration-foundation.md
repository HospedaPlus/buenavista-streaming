# Kafka migration foundation

## Goal

Move Buenavista's business-event transport from RabbitMQ to Apache Kafka. Events
must originate from the canonical HospedaPlus simulated data in
[`HospedaPlus/db_simulated`](https://github.com/HospedaPlus/db_simulated), and
later from project APIs. Independent consumers will deliver applicable events
to Neon PostgreSQL and the Amazon S3 Bronze layer.

## Target architecture

```text
db_simulated seed ─┐
                   ├─> producers + Pydantic validation ─> Kafka
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

## Scope and migration rules

1. Keep `db_simulated` as the canonical source for hotel, room-type, channel,
   and other seeded catalog values. Do not create a competing catalog in this
   repository.
2. Start with `reservation.events.v1_2`. Add hotel, inventory, and rate topics only
   when their event contracts and producers are ready.
3. Keep Neon and S3 as independent consumer groups so a failure in one sink
   does not directly stop the other.
4. Design consumers for at-least-once delivery, idempotent writes, bounded
   retries, and recoverable failures. Do not claim exactly-once writes across
   Kafka, Neon, and S3.
5. Commit consumer offsets only after the corresponding sink write succeeds.
6. Do not blindly write all event types to the operational `reservations`
   table. Map event types to the existing database schema and business rules.
7. Preserve the existing RabbitMQ compose file, volume, queues, and pending
   messages while the Kafka path is developed and validated.

## Local Kafka broker

A separate `docker-compose.kafka.yml` provides a single-node Kafka KRaft
broker for development. It is not a production configuration and does not
replace the existing RabbitMQ setup yet.

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

Stop only this Kafka service when finished:

```powershell
docker compose -f docker-compose.kafka.yml down
```

The named `kafka_data` volume is intentionally retained by this command. Do not
add `-v` unless deleting local Kafka data is explicitly intended.

## Implementation sequence

- [x] Create an isolated local Kafka broker configuration.
- [x] Document the target architecture and migration safeguards.
- [ ] Add Kafka client dependency and configuration.
- [ ] Implement a Kafka producer using the current validated event contract.
- [ ] Implement and test a Kafka consumer with explicit offset management.
- [ ] Align hotel IDs, room types, currencies, and channels with `db_simulated`.
- [ ] Add isolated integration tests for publication and consumption.
- [ ] Implement an idempotent Neon consumer.
- [ ] Implement a durable S3 Bronze consumer with ingestion metadata.
- [ ] Document deployment and evidence; decide whether RabbitMQ remains needed
      elsewhere before removing it.

## Important limitations

The current RabbitMQ producer/consumer code is not converted by adding this
broker file alone. The Kafka producer, consumer, tests, Neon persistence, and S3
persistence remain follow-up implementation tasks. This local KRaft broker uses
one node and replication factor 1, so it is for development and demonstrations,
not production availability.
