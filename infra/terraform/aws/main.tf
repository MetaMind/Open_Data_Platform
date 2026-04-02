terraform {
  required_version = ">= 1.5"
  required_providers {
    aws = {
      source  = "hashicorp/aws"
      version = "~> 5.0"
    }
    random = {
      source  = "hashicorp/random"
      version = "~> 3.0"
    }
  }
}

provider "aws" {
  region = var.aws_region
}

# Deploy OpenMeta stack (DataHub + Marquez) when metadata_engine = "openmeta"
resource "aws_ecs_cluster" "datahub" {
  count = var.metadata_engine == "openmeta" ? 1 : 0
  name  = "odep-datahub-${var.environment}"
}

# Configure MetaMind connection when metadata_engine = "metamind"
resource "aws_secretsmanager_secret" "metamind_api_key" {
  count = var.metadata_engine == "metamind" ? 1 : 0
  name  = "odep/${var.environment}/metamind-api-key"
}

# Deploy Airflow (MWAA) when orchestration_engine = "airflow"
resource "aws_mwaa_environment" "airflow" {
  count = var.orchestration_engine == "airflow" ? 1 : 0
  name  = "odep-airflow-${var.environment}"

  airflow_version    = "2.8.1"
  execution_role_arn = aws_iam_role.mwaa_execution[0].arn
  source_bucket_arn  = aws_s3_bucket.mwaa_dags[0].arn
  dag_s3_path        = "dags/"

  network_configuration {
    security_group_ids = []
    subnet_ids         = []
  }
}

resource "aws_iam_role" "mwaa_execution" {
  count = var.orchestration_engine == "airflow" ? 1 : 0
  name  = "odep-mwaa-execution-${var.environment}"

  assume_role_policy = jsonencode({
    Version = "2012-10-17"
    Statement = [{
      Action    = "sts:AssumeRole"
      Effect    = "Allow"
      Principal = { Service = "airflow.amazonaws.com" }
    }]
  })
}

resource "aws_s3_bucket" "mwaa_dags" {
  count  = var.orchestration_engine == "airflow" ? 1 : 0
  bucket = "odep-mwaa-dags-${var.environment}-${random_id.suffix[0].hex}"
}

resource "random_id" "suffix" {
  count       = var.orchestration_engine == "airflow" ? 1 : 0
  byte_length = 4
}
