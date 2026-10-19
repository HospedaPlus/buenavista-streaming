# AWS deployment migration (proposal)

This guide explains the first cloud deployment in plain language. It is a proposed path, not authorization to provision shared AWS resources. AWS account, region, network owner, and non-production budget still need to be confirmed.

## The hotel-to-cloud translation

- **Amazon MSK** runs the Kafka broker and stores the event topics.
- **ECS/Fargate** runs our Python program in containers. We run the producer, Neon projector, and Bronze capture as separate tasks.
- **Amazon S3** stores the OLAP Bronze files.
- **Neon PostgreSQL** stores the operational projection in the additive F2 schema.
- **IAM task roles** give each task limited AWS permissions without placing long-lived cloud keys in the image.

## Proposed runtime

- Amazon MSK for `buenavista.reservations.v1`, retry, and dead-letter topics.
- Separate ECS/Fargate tasks for the synthetic producer, Neon OLTP projector, and S3 Bronze capture.
- Amazon S3 for append-only Bronze objects partitioned by ingest date and hotel ID.
- Airflow → Silver → Gold → Power BI stays with the data-platform workstream.

MSK Serverless still has hourly cluster and partition charges plus data and storage charges. It is not automatically the cheapest option. Compare at least two architectures in the [AWS Pricing Calculator](https://calculator.aws/) using measured partitions, event volume, retention, and task hours before picking one.

## Authentication: AWS IAM for MSK

The AWS container uses `KAFKA_AUTH_MODE=msk_iam`, `KAFKA_SECURITY_PROTOCOL=SASL_SSL`, and `AWS_REGION`. Its short-lived Kafka token is signed using credentials from the ECS task role. No static Kafka username or password is needed in AWS.

The Docker image installs the AWS MSK signer through the `aws` optional dependency. The three clients (producer, Neon consumer, Bronze consumer) share this Kafka authentication configuration. The IAM policies should be separate and scoped to the required cluster, topics, and consumer groups:

- Producer: connect and write to the main topic.
- Neon projector: read main and retry topics, join its consumer group, and write retry/DLQ topics.
- Bronze capture: read the main topic and join its consumer group.
- All tasks: only the service actions they individually need. The Bronze task additionally writes its specific S3 prefix; the Neon task gets the Neon secret.

ECS task roles are designed to provide AWS credentials to application code that uses AWS services. Give each task its own least-privilege role and keep the Neon TLS connection URL only in the OLTP task's approved secret store. [AWS ECS task role documentation](https://docs.aws.amazon.com/AmazonECS/latest/developerguide/task-iam-roles.html).

## Build and run the same container

From the repository root:

```sh
docker build -t hospedaplus-f2-streaming:<tag> .
```

Configure three tasks from this image:

- Producer: `python -m src.producer --continuous`
- OLTP projection: `python -m src.consumer`
- Bronze capture: `python -m src.bronze_consumer`

Use an immutable image digest after scanning and promotion. Start the producer as an on-demand task for a controlled demonstration. Configure task sizing, restart behavior, health checks, and scaling after measuring the workload.

## Network and data controls

- Place MSK and Fargate tasks in the approved private network. Fargate tasks use network interfaces and require subnet and security-group configuration. [Fargate networking details](https://docs.aws.amazon.com/AmazonECS/latest/developerguide/fargate-tasks.html).
- Restrict Kafka ingress to those task security groups and require TLS.
- Give Bronze capture access only to its assigned S3 prefix. Enable S3 Block Public Access, default encryption, and owner-approved lifecycle and retention policies.
- Keep the synthetic event contract free of direct guest identifiers and payment information. Silver pseudonymizes before analytics.
- Neon remains external to AWS; document the approved TLS network path from the task to Neon.

## Step-by-step rollout

1. **Learn locally:** follow [Kafka from zero](../../docs/kafka-from-zero.md) and see two independent consumer groups read the same topic.
2. **Confirm the environment:** AWS account, region, network owner, non-production budget, topic partition/retention assumptions, Neon connection path, and data retention.
3. **Package:** build the Docker image; run the producer and consumers locally; scan and publish the image to the approved ECR registry.
4. **Review the database change:** inspect `sql/streaming_schema.sql`; apply only through the migration process to a disposable Neon branch first. Confirm F3-owned tables stay unchanged.
5. **Provision non-production:** MSK cluster/topics, encrypted S3 bucket, task roles, ECS tasks, logs/alarms, and required secrets/networking using reviewed Infrastructure as Code.
6. **Validate the complete flow:** verify independent groups, commit-after-write behavior, deduplication, retry/DLQ, Bronze partitions/coordinates, task restart, and graceful shutdown.
7. **Review cost and operations:** compare at least two options in the AWS Pricing Calculator with measured usage. Approve logs, alarms, recovery, owners, cleanup, rollback, and production cutover.
8. **Publish agreed documentation:** Vane uploads approved documentation changes to Docusaurus.

## What is not deployed

No AWS resources have been provisioned, and the Neon migration has not been applied. We still need the approved AWS account, region, network owner, and budget before provisioning. AWS resources can accrue costs while running, so the non-production plan must include cost review and a cleanup step.
