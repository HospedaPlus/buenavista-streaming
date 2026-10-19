# AWS access and budget readiness 🛡️

Use this before any cloud deployment. The goal is to get access to an **approved HospedaPlus non-production account**, not to create personal cloud infrastructure without the project owner's agreement.

## What to request from the project owner

- [ ] Confirm whether HospedaPlus already has an AWS Organization or a project/school sandbox account.
- [ ] Request named user access through IAM Identity Center (AWS access portal), if available. Do not request or share the root account password or root access keys.
- [ ] Confirm the AWS account/alias and the approved region.
- [ ] Confirm an existing VPC, at least two subnets in distinct Availability Zones for Amazon MSK Serverless, and who owns the network/security groups. [MSK Serverless subnet requirement](https://docs.aws.amazon.com/msk/latest/developerguide/create-serverless-cluster.html).
- [ ] Confirm how non-production tasks reach Amazon MSK, ECR, CloudWatch, STS, S3, and the external Neon endpoint.
- [ ] Set a monthly cost ceiling and a billing/budget alert recipient before creating resources. MSK Serverless charges for cluster hours, partition hours, data, and storage; the cluster can continue to incur charges when the producer and consumers are stopped. [Amazon MSK pricing](https://aws.amazon.com/msk/pricing/).
- [ ] Confirm who can review the Terraform plan and who is authorized to apply it and destroy the non-production resources.
- [ ] Confirm the approved Terraform state location and ownership before sharing state between team members.
- [ ] Ask the Neon owner to create/approve a non-production secret for `DATABASE_URL`. Share only the secret ARN and connection policy with the deployment owner; never paste the password into this repository or chat.

## If the owner asks you to create a standalone account

Pause and confirm that the project sponsor authorizes the billing account and will own its costs. Secure the root identity with MFA; do not create root access keys. Use a named IAM Identity Center or role-based identity for everyday work and temporary credentials where possible. [AWS root-user best practices](https://docs.aws.amazon.com/IAM/latest/UserGuide/root-user-best-practices.html), [AWS administrator sign-in best practices](https://docs.aws.amazon.com/signin/latest/userguide/best-practices-admin.html).

## Copyable access request

> Hi, I’m preparing the HospedaPlus F2 Kafka streaming deployment. Before we create any cloud resources, does HospedaPlus already have an approved AWS non-production account or project sandbox I can use?  
>
> If so, could you please confirm:
> 1. the account/portal and how I should request IAM Identity Center access;
> 2. the allowed AWS region and monthly budget ceiling;
> 3. the VPC and two subnet IDs in separate Availability Zones, plus the network owner;
> 4. who will review the Terraform plan and approve an apply/destroy;
> 5. the approved Terraform state location; and
> 6. who can provide a non-production Neon secret ARN and approve the additive F2 schema?
>
> I have not created or deployed AWS resources. I’m first validating the Kafka flow locally and preparing the deployment files for review.

## Current project status

- Local Kafka is available for learning with Docker Compose.
- The repository's AWS authentication path uses Amazon MSK IAM and an ECS task role; no static Kafka password is needed in the cloud.
- The AWS deployment guide is a proposal. There is no approved AWS account, account access, or infrastructure deployment yet.
- Neon schema changes remain un-applied.

## What we can do without AWS access

Follow [Kafka from zero](../kafka-from-zero.md) to publish sample events and read them from two independent groups. We can review and prepare infrastructure templates while access is pending, but a real `terraform plan` needs the project's account, region, and network inputs; `terraform apply` must wait for the owner's cost and change approval.
