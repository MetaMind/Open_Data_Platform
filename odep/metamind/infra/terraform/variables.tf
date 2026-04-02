variable "environment" {
  description = "Deployment environment (dev, staging, prod)"
  type        = string
  default     = "prod"
}

variable "region" {
  description = "AWS region"
  type        = string
  default     = "us-east-1"
}

variable "vpc_cidr" {
  description = "CIDR block for the VPC"
  type        = string
  default     = "10.0.0.0/16"
}

variable "eks_cluster_name" {
  description = "EKS cluster name"
  type        = string
  default     = "metamind-cluster"
}

variable "eks_node_instance_types" {
  description = "EC2 instance types for the query node group"
  type        = list(string)
  default     = ["c5.2xlarge"]
}

variable "eks_min_nodes" {
  description = "Minimum number of query nodes"
  type        = number
  default     = 3
}

variable "eks_max_nodes" {
  description = "Maximum number of query nodes"
  type        = number
  default     = 15
}

variable "db_instance_class" {
  description = "RDS instance class"
  type        = string
  default     = "db.r6g.xlarge"
}

variable "db_name" {
  description = "PostgreSQL database name"
  type        = string
  default     = "metamind"
}

variable "db_username" {
  description = "PostgreSQL admin username"
  type        = string
  default     = "metamind_admin"
}

variable "db_password" {
  description = "PostgreSQL admin password"
  type        = string
  sensitive   = true
}

variable "redis_node_type" {
  description = "ElastiCache node type"
  type        = string
  default     = "cache.r6g.large"
}

variable "redis_num_replicas" {
  description = "Number of Redis read replicas"
  type        = number
  default     = 2
}

variable "kafka_broker_count" {
  description = "Number of MSK Kafka brokers"
  type        = number
  default     = 3
}

variable "kafka_instance_type" {
  description = "MSK broker instance type"
  type        = string
  default     = "kafka.m5.large"
}

variable "s3_data_lake_bucket" {
  description = "S3 bucket name for Iceberg data lake"
  type        = string
  default     = "metamind-data-lake"
}

variable "s3_state_bucket" {
  description = "S3 bucket for Terraform remote state"
  type        = string
  default     = "metamind-terraform-state"
}

variable "metamind_namespace" {
  description = "Kubernetes namespace for MetaMind workloads"
  type        = string
  default     = "metamind"
}

variable "metamind_image_tag" {
  description = "MetaMind container image tag"
  type        = string
  default     = "latest"
}

variable "availability_zones" {
  description = "Availability zones to deploy into"
  type        = list(string)
  default     = ["us-east-1a", "us-east-1b", "us-east-1c"]
}

variable "private_subnet_cidrs" {
  description = "CIDR blocks for private subnets (one per AZ)"
  type        = list(string)
  default     = ["10.0.1.0/24", "10.0.2.0/24", "10.0.3.0/24"]
}

variable "public_subnet_cidrs" {
  description = "CIDR blocks for public subnets (one per AZ)"
  type        = list(string)
  default     = ["10.0.101.0/24", "10.0.102.0/24", "10.0.103.0/24"]
}

variable "rds_backup_retention_days" {
  description = "RDS automated backup retention period"
  type        = number
  default     = 7
}

variable "tags" {
  description = "Common resource tags"
  type        = map(string)
  default = {
    Project   = "MetaMind"
    ManagedBy = "Terraform"
  }
}
