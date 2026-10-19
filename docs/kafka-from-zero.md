# Kafka from zero: HospedaPlus edition 🏨

This guide starts with the Kafka ideas you need for the F2 streaming deployment, then gives you a local exercise. You do not need to know Kafka before you begin.

## 1. Kafka in one sentence

**Kafka is a durable event log that lets one system publish events and lets multiple independent systems read them at their own pace.**

Imagine the hotel front desk records each reservation change in a shared, numbered journal. The operations team and analytics team can each read that journal independently, remember where they stopped, and catch up later. Kafka is the journal.

Kafka is not the reservation database, and it does not automatically make the data correct. Our Python producer creates and validates events; Kafka stores and distributes them; our consumers save or transform them.

## 2. Learn the words using our hotel

| Kafka word | In this project | Hotel analogy |
|---|---|---|
| **Event** | One reservation change, such as `reservation.created` | A note that a guest's stay was booked or changed |
| **Producer** | `src/producer.py` | The staff member writing a note into the shared journal |
| **Topic** | `buenavista.reservations.v1` | The journal for reservation events |
| **Message key** | `reservation_id` | The room/reservation identifier used to keep related changes together |
| **Partition** | One ordered slice of a topic | A numbered journal section; same-key events go to the same section |
| **Offset** | A message's position in one partition | The page number / bookmark |
| **Consumer** | `src/consumer.py` or `src/bronze_consumer.py` | A reader who processes journal entries |
| **Consumer group** | `buenavista-neon-projector-v1` or `buenavista-bronze-capture-v1` | A team with its own bookmark |
| **Commit** | Save the consumer's next offset | Record the team's bookmark after its work is safely saved |
| **Retry / DLQ** | Retry and dead-letter topics | Try a temporary issue again; set aside invalid or exhausted entries for review |

A topic can have multiple consumer groups. Each group keeps its own bookmark, so the OLTP and OLAP paths can each process the same event. Inside one group, multiple consumer instances share the work. Messages with the same key are assigned consistently to a partition, which preserves order for that key. [Kafka's documentation explains topics, partitions, keys, and consumer groups.](https://kafka.apache.org/documentation/)

## 3. Our flow, in plain language

1. The simulator invents a reservation lifecycle event. Guest data is synthetic; direct guest identifiers and payment details are excluded.
2. The Python producer validates the event against schema v1.1 and publishes it to `buenavista.reservations.v1`, keyed by `reservation_id`.
3. The **OLTP consumer group** writes an event inbox and a current reservation projection to the additive Neon `streaming` schema. It deduplicates by `event_id` and keeps only a newer `reservation_version`. It commits its Kafka offset after the Neon transaction succeeds.
4. The **OLAP consumer group** writes raw event batches to S3 Bronze, partitioned by `ingest_date` and `hotel_id`, with event time, ingest time, and Kafka coordinates. It commits only after the objects are written.
5. Airflow, Silver, Gold, and Power BI are later analytics steps owned by the data-platform workstream.

**Important:** Kafka retains a message according to topic policy; reading it does not delete it. This makes independent consumers and replay possible. Our consumers use at-least-once processing, so they must tolerate a replay.

## 4. Try the stream on your computer

This is a local learning exercise. It starts a single development broker in Docker; it does not create AWS resources or change Neon.

### Start Kafka

From the repository folder in PowerShell:

```powershell
Copy-Item .env.example .env
docker compose up -d kafka
docker compose ps
```

The local broker is KRaft-based and uses PLAINTEXT for local development. That is intentionally different from the authenticated AWS deployment.

Create the topics once (the full commands are in [the runbook](runbook.md)):

```powershell
docker compose exec kafka kafka-topics --bootstrap-server kafka:29092 --create --if-not-exists --topic buenavista.reservations.v1 --partitions 6 --replication-factor 1
docker compose exec kafka kafka-topics --bootstrap-server kafka:29092 --create --if-not-exists --topic buenavista.reservations.retry.v1 --partitions 6 --replication-factor 1
docker compose exec kafka kafka-topics --bootstrap-server kafka:29092 --create --if-not-exists --topic buenavista.reservations.dlq.v1 --partitions 6 --replication-factor 1
```

### Publish a few sample events

Create and activate a Python environment and install the project as described in the [runbook](runbook.md). Then publish 20 synthetic events:

```powershell
python -m src.producer --count 20 --seed 42
```

The producer prints delivery information. That means Kafka accepted the events; it does not yet mean Neon or S3 processed them.

### Read the same topic as two independent groups

In a terminal, read a few events as the operations group:

```powershell
docker compose exec kafka kafka-console-consumer --bootstrap-server kafka:29092 --topic buenavista.reservations.v1 --group learn-operations --from-beginning --max-messages 5
```

Run the same command in a second terminal, changing only the group:

```powershell
docker compose exec kafka kafka-console-consumer --bootstrap-server kafka:29092 --topic buenavista.reservations.v1 --group learn-analytics --from-beginning --max-messages 5
```

You should see events in both terminals. Each group has its own bookmark. This is the central idea behind the two branches in our diagram.

To see the topic and its partitions:

```powershell
docker compose exec kafka kafka-topics --bootstrap-server kafka:29092 --describe --topic buenavista.reservations.v1
```

Stop the broker when you are done:

```powershell
docker compose down
```

These console-reader groups are just for learning. The real Neon consumer needs a database with the reviewed F2 schema, and the Bronze consumer needs either a local output directory or an approved S3 bucket. Do not point a first experiment at shared Neon.

## 5. What “deploy the stream” means

Deployment means running the same producer and consumer programs on managed cloud infrastructure, with secure connections, permissions, logs, restart behavior, and recovery instructions.

| Piece | Proposed deployment role |
|---|---|
| **Amazon MSK** | Managed Kafka broker for the main, retry, and DLQ topics |
| **ECS/Fargate tasks** | Run the producer, Neon projector, and Bronze capture separately |
| **Amazon S3** | Store append-only Bronze events |
| **Neon PostgreSQL** | Store the operational projection in the additive F2 schema |
| **CloudWatch** | Collect logs and alarms for the AWS tasks |

Each consumer gets its own task and its own IAM role. For example, the Bronze task needs S3 write permission for its assigned prefix; it should not receive Neon database access. AWS ECS task roles provide short-lived AWS credentials to application code. In our AWS path, the role also authenticates Kafka access to Amazon MSK, avoiding static Kafka passwords. [AWS ECS task role guidance](https://docs.aws.amazon.com/AmazonECS/latest/developerguide/task-iam-roles.html).

### The deployment steps we will work through

- [ ] **Learn locally:** publish events and read them from two groups (the exercise above).
- [ ] **Package the application:** build the Docker image and run each of the three commands locally.
- [ ] **Choose the AWS setup:** confirm account, region, VPC/network owner, topic retention, authentication mode, and the non-production budget.
- [ ] **Review costs and security:** compare at least two workload-sized options in the AWS Pricing Calculator. MSK Serverless has cluster-hour and partition-hour charges in addition to data and storage charges; “serverless” does not mean “free” or automatically cheapest. [Current Amazon MSK pricing](https://aws.amazon.com/msk/pricing/).
- [ ] **Prepare non-production resources:** broker, topics, image registry, S3 bucket, task roles, logs, and secrets using reviewed infrastructure code.
- [ ] **Test the complete path:** producer → both consumer groups → Neon and S3, including replay, retry/DLQ, and task restart.
- [ ] **Get owner approval before production:** review the additive Neon schema, catalog mapping, costs, monitoring, recovery, and rollback plan.

The full AWS proposal and current blockers are in [`deploy/aws/README.md`](../deploy/aws/README.md). The Docker image includes the Amazon MSK IAM signer; local development remains unauthenticated. **We have not deployed AWS infrastructure or applied the Neon migration.** We will build and review the artifacts first; cloud provisioning needs the approved AWS environment and budget.

## 6. What belongs to our team

This repository owns the F2 event producer, schema contract, Kafka flow, Neon projection consumer, and S3 Bronze capture. It does not own F3 database tables, the public F5/F6/F7 APIs, or the Airflow/Silver/Gold models. Keep Melissa's 28-hotel catalog as the source of truth, align channel and room-type values before analytics joins, and send approved documentation changes to Vane for Docusaurus upload.

## Next step

Start with **Section 4** and tell me what you see after you run the two consumer-group commands. If Docker or Python shows an error, copy the error text here and I’ll help you fix it.
