# Production runtime on AWS for the daily cost showback.
#   EventBridge Scheduler -> ECS Fargate task -> Airflow DAG -> Snowflake ACCOUNT_USAGE
#   Snowflake key in Secrets Manager, reports to S3 (dated + latest), failures to SNS, logs in CloudWatch.

terraform {
  required_version = ">= 1.6"
  required_providers {
    aws = {
      source  = "hashicorp/aws"
      version = "~> 6.0"
    }
  }
}

provider "aws" {
  region = var.region
  default_tags {
    tags = { project = "snowflake-cost-attribution", managed_by = "terraform" }
  }
}

data "aws_caller_identity" "me" {}

data "aws_vpc" "default" {
  default = true
}

data "aws_subnets" "default" {
  filter {
    name   = "vpc-id"
    values = [data.aws_vpc.default.id]
  }
}

locals {
  name   = "finops-showback"
  bucket = "${local.name}-${data.aws_caller_identity.me.account_id}"
}

# ---------- storage: code bundle and reports ----------
resource "aws_s3_bucket" "reports" {
  bucket = local.bucket
}

resource "aws_s3_bucket_public_access_block" "reports" {
  bucket                  = aws_s3_bucket.reports.id
  block_public_acls       = true
  block_public_policy     = true
  ignore_public_acls      = true
  restrict_public_buckets = true
}

resource "aws_s3_bucket_versioning" "reports" {
  bucket = aws_s3_bucket.reports.id
  versioning_configuration {
    status = "Enabled"
  }
}

resource "aws_s3_bucket_server_side_encryption_configuration" "reports" {
  bucket = aws_s3_bucket.reports.id
  rule {
    apply_server_side_encryption_by_default {
      sse_algorithm = "AES256"
    }
  }
}

resource "aws_s3_bucket_lifecycle_configuration" "reports" {
  bucket = aws_s3_bucket.reports.id
  rule {
    id     = "expire-old-reports"
    status = "Enabled"
    filter {
      prefix = "reports/"
    }
    expiration {
      days = 400
    }
    noncurrent_version_expiration {
      noncurrent_days = 30
    }
  }
}

resource "aws_s3_object" "code" {
  bucket      = aws_s3_bucket.reports.id
  key         = "code/finops.zip"
  source      = "${path.module}/../../dist/finops.zip"
  source_hash = filemd5("${path.module}/../../dist/finops.zip")
}

# ---------- secrets and alerts ----------
# The value is put with the CLI (see README), so the private key never enters Terraform state.
resource "aws_secretsmanager_secret" "snowflake_key" {
  name                    = "${local.name}/snowflake-private-key"
  description             = "PKCS#8 key for Snowflake user FINOPS_SVC"
  recovery_window_in_days = 7
}

resource "aws_sns_topic" "alerts" {
  name = "${local.name}-alerts"
}

resource "aws_sns_topic_subscription" "email" {
  count     = var.alert_email == "" ? 0 : 1
  topic_arn = aws_sns_topic.alerts.arn
  protocol  = "email"
  endpoint  = var.alert_email
}

# ---------- compute ----------
resource "aws_cloudwatch_log_group" "job" {
  name              = "/ecs/${local.name}"
  retention_in_days = 30
}

resource "aws_ecs_cluster" "this" {
  name = local.name
}

resource "aws_security_group" "task" {
  name        = "${local.name}-task"
  description = "Outbound only: Snowflake, S3, PyPI"
  vpc_id      = data.aws_vpc.default.id
  egress {
    from_port   = 0
    to_port     = 0
    protocol    = "-1"
    cidr_blocks = ["0.0.0.0/0"]
  }
}

data "aws_iam_policy_document" "ecs_tasks_trust" {
  statement {
    actions = ["sts:AssumeRole"]
    principals {
      type        = "Service"
      identifiers = ["ecs-tasks.amazonaws.com"]
    }
    condition {
      test     = "StringEquals"
      variable = "aws:SourceAccount"
      values   = [data.aws_caller_identity.me.account_id]
    }
  }
}

resource "aws_iam_role" "execution" {
  name               = "${local.name}-execution"
  assume_role_policy = data.aws_iam_policy_document.ecs_tasks_trust.json
}

resource "aws_iam_role_policy_attachment" "execution" {
  role       = aws_iam_role.execution.name
  policy_arn = "arn:aws:iam::aws:policy/service-role/AmazonECSTaskExecutionRolePolicy"
}

# Least privilege for the job itself: read its code, write reports, read one secret, publish one topic.
data "aws_iam_policy_document" "task" {
  statement {
    actions   = ["s3:GetObject"]
    resources = ["${aws_s3_bucket.reports.arn}/code/*"]
  }
  statement {
    actions   = ["s3:PutObject"]
    resources = ["${aws_s3_bucket.reports.arn}/reports/*"]
  }
  statement {
    actions   = ["secretsmanager:GetSecretValue"]
    resources = [aws_secretsmanager_secret.snowflake_key.arn]
  }
  statement {
    actions   = ["sns:Publish"]
    resources = [aws_sns_topic.alerts.arn]
  }
}

resource "aws_iam_role" "task" {
  name               = "${local.name}-task"
  assume_role_policy = data.aws_iam_policy_document.ecs_tasks_trust.json
}

resource "aws_iam_role_policy" "task" {
  role   = aws_iam_role.task.id
  policy = data.aws_iam_policy_document.task.json
}

resource "aws_ecs_task_definition" "job" {
  family                   = local.name
  requires_compatibilities = ["FARGATE"]
  network_mode             = "awsvpc"
  cpu                      = "1024"
  memory                   = "3072"
  execution_role_arn       = aws_iam_role.execution.arn
  task_role_arn            = aws_iam_role.task.arn
  runtime_platform {
    operating_system_family = "LINUX"
    cpu_architecture        = "ARM64"
  }
  container_definitions = jsonencode([{
    name      = "job"
    image     = "public.ecr.aws/docker/library/python:3.12-slim"
    essential = true
    command = ["bash", "-c", join(" && ", [
      "pip install -q boto3",
      "python -c \"import boto3,io,zipfile; b=boto3.client('s3').get_object(Bucket='${aws_s3_bucket.reports.id}',Key='code/finops.zip')['Body'].read(); zipfile.ZipFile(io.BytesIO(b)).extractall('/app')\"",
      "bash /app/aws/entrypoint.sh",
    ])]
    environment = [
      { name = "JOB", value = "showback" },
      { name = "REPORTS_BUCKET", value = aws_s3_bucket.reports.id },
      { name = "ALERT_TOPIC_ARN", value = aws_sns_topic.alerts.arn },
      { name = "SNOWFLAKE_KEY_SECRET", value = aws_secretsmanager_secret.snowflake_key.arn },
      { name = "SNOWFLAKE_ACCOUNT", value = var.snowflake_account },
    ]
    logConfiguration = {
      logDriver = "awslogs"
      options = {
        awslogs-group         = aws_cloudwatch_log_group.job.name
        awslogs-region        = var.region
        awslogs-stream-prefix = "job"
      }
    }
  }])
}

# ---------- schedules ----------
data "aws_iam_policy_document" "scheduler_trust" {
  statement {
    actions = ["sts:AssumeRole"]
    principals {
      type        = "Service"
      identifiers = ["scheduler.amazonaws.com"]
    }
    condition {
      test     = "StringEquals"
      variable = "aws:SourceAccount"
      values   = [data.aws_caller_identity.me.account_id]
    }
  }
}

resource "aws_iam_role" "scheduler" {
  name               = "${local.name}-scheduler"
  assume_role_policy = data.aws_iam_policy_document.scheduler_trust.json
}

data "aws_iam_policy_document" "scheduler" {
  statement {
    actions   = ["ecs:RunTask"]
    resources = ["${trimsuffix(aws_ecs_task_definition.job.arn, ":${aws_ecs_task_definition.job.revision}")}:*"]
  }
  statement {
    actions   = ["iam:PassRole"]
    resources = [aws_iam_role.execution.arn, aws_iam_role.task.arn]
  }
}

resource "aws_iam_role_policy" "scheduler" {
  role   = aws_iam_role.scheduler.id
  policy = data.aws_iam_policy_document.scheduler.json
}

locals {
  schedules = {
    showback = { cron = "cron(0 6 * * ? *)", job = "showback" }       # daily, after ACCOUNT_USAGE latency
    workload = { cron = "cron(0 8 ? * MON-FRI *)", job = "workload" } # weekday scripted workload session
  }
}

resource "aws_scheduler_schedule" "job" {
  for_each                     = local.schedules
  name                         = "${local.name}-${each.key}"
  schedule_expression          = each.value.cron
  schedule_expression_timezone = "UTC"
  flexible_time_window {
    mode = "OFF"
  }
  target {
    arn      = aws_ecs_cluster.this.arn
    role_arn = aws_iam_role.scheduler.arn
    ecs_parameters {
      task_definition_arn = aws_ecs_task_definition.job.arn
      launch_type         = "FARGATE"
      network_configuration {
        subnets          = data.aws_subnets.default.ids
        security_groups  = [aws_security_group.task.id]
        assign_public_ip = true
      }
    }
    input = jsonencode({
      containerOverrides = [{ name = "job", environment = [{ name = "JOB", value = each.value.job }] }]
    })
    retry_policy {
      maximum_retry_attempts = 1
    }
  }
}

# ---------- outputs ----------
output "bucket" {
  value = aws_s3_bucket.reports.id
}

output "secret_arn" {
  value = aws_secretsmanager_secret.snowflake_key.arn
}

output "run_now" {
  description = "Start a showback run by hand"
  value       = "aws ecs run-task --cluster ${aws_ecs_cluster.this.name} --launch-type FARGATE --task-definition ${aws_ecs_task_definition.job.family} --network-configuration 'awsvpcConfiguration={subnets=[${join(",", data.aws_subnets.default.ids)}],securityGroups=[${aws_security_group.task.id}],assignPublicIp=ENABLED}'"
}
