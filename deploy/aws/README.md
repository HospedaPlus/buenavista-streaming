# AWS deployment migration (proposal)

This guide describes a staged target for the approved F2 flow. It is a deployment plan, not Infrastructure as Code and not authorization to provision shared AWS resources. No AWS account, region, VPC, cluster, bucket, or production secret is embedded here.

## Proposed runtime

- Amazon MSK for the Kafka topic, retry topic, and DLQ.
- Three independently managed ECS/Fargate task roles and services: synthetic producer, Neon OLTP projector, and S3 Bronze capture. The producer may run as an on-demand task for demonstrations instead of a continuously running service.
- Amazon S3 for append-only Bronze objects, partitioned by ingest date and hotel ID.
- Neon remains the external OLTP database. The Airflow → Silver → Gold → Power BI path remains owned by the data-platform workstream.

This is a starting design, not a claim that MSK Serverless or provisioned MSK is cheapest. Compare at least two broker/runtime options with explicit throughput, retention, availability, task hours, and data-volume assumptions in AWS Pricing Calculator before selecting a topology.

## Runtime image

Build the image from the repository root and publish it to the team's approved image registry:

```sh
docker build -t hospedaplus-f2-streaming:<tag> .
```

Run the same immutable image as separate tasks with these commands:

- Producer: `python -m src.producer --continuous`
- OLTP projection: `python -m src.consumer`
- Bronze capture: `python -m src.bronze_consumer`

Use a pinned image digest in task definitions after the image has been scanned and promoted. Configure CPU, memory, task count, health checks, and scale limits from measured load. Avoid running more than one synthetic producer unless duplicate test traffic is intended.

## Required environment and secrets

Set the Kafka bootstrap endpoints, approved topic names, consumer group names, and `KAFKA_SECURITY_PROTOCOL=SASL_SSL`. The current Python clients support SASL mechanism, username, and password settings (`KAFKA_SASL_MECHANISM`, `KAFKA_SASL_USERNAME`, `KAFKA_SASL_PASSWORD`) plus an optional CA path. Select an MSK authentication mode compatible with these settings and validate it end-to-end before deployment; IAM-based MSK authentication is not wired into this Python client yet.

Inject Kafka credentials and `DATABASE_URL` from the approved secret manager at task start. Do not put secret values in task definitions, image layers, GitHub variables, logs, or this repository. Use TLS for Neon. Configure the Bronze task to use its ECS task role for S3 rather than static AWS keys.

## AWS and network controls

- Run tasks in private subnets with egress only to required endpoints/destinations. Place MSK in the approved private network and restrict broker ingress to the task security groups.
- Use separate least-privilege task roles. Only the Bronze task needs S3 object writes under its assigned Bronze prefix; restrict secret reads to the task that uses each secret.
- Enable encryption in transit for Kafka and Neon; use S3 Block Public Access and default encryption. Define retention, lifecycle, recovery, and deletion policies with data owners.
- Keep guest data synthetic. Silver must pseudonymize before analytics; do not copy direct guest identifiers into Bronze by expanding the event contract.

## Staged migration gates

1. **Contract and target:** confirm schema v1.1, topic names/partition count/retention, F3 channel and room-type catalog mapping, AWS region/account, network ownership, MSK auth mode, Neon connection path, and data-retention requirements.
2. **Image:** build and scan the container; smoke-run producer, OLTP consumer, and Bronze consumer against an isolated non-production Kafka cluster.
3. **Data destinations:** obtain review of `sql/streaming_schema.sql`; apply it only through the team's approved migration workflow to a disposable Neon branch first. Validate replay/idempotency and prove F3 tables are unchanged. Create a non-production encrypted S3 bucket and lifecycle policy.
4. **Infrastructure:** provision non-production MSK, topics, IAM/task roles, ECS tasks, logs/alarms, and secrets from approved IaC maintained by the infrastructure owner.
5. **End-to-end validation:** confirm both consumer groups independently process events; verify offset-after-commit behavior, replay handling, DLQ visibility, Bronze partition layout and coordinates, task restarts, and controlled shutdown.
6. **Cost and operations review:** compare at least two options in AWS Pricing Calculator using measured event volume and retention; approve alarms, ownership, runbooks, rollback, and production cutover.
7. **Documentation:** keep this repository's source docs current. Vane uploads the agreed documentation changes to Docusaurus after the team approves them.

## Current blockers before provisioning

This repository does not yet contain the organization's approved IaC pipeline or AWS account/VPC inputs. The team still needs to select MSK authentication compatible with the current Python client, define non-production and production sizing/retention, settle the F3 catalog mapping, review the additive Neon schema, and provide the approved secret and network paths. Resolve these inputs before opening a deploy workflow or applying infrastructure.
