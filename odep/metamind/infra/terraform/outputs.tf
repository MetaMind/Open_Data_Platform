output "eks_cluster_endpoint" {
  description = "EKS cluster API server endpoint"
  value       = aws_eks_cluster.main.endpoint
}

output "eks_cluster_name" {
  description = "EKS cluster name"
  value       = aws_eks_cluster.main.name
}

output "eks_cluster_ca" {
  description = "EKS cluster certificate authority data (base64)"
  value       = aws_eks_cluster.main.certificate_authority[0].data
  sensitive   = true
}

output "rds_endpoint" {
  description = "RDS PostgreSQL endpoint hostname"
  value       = aws_db_instance.postgres.address
}

output "rds_port" {
  description = "RDS PostgreSQL port"
  value       = aws_db_instance.postgres.port
}

output "redis_endpoint" {
  description = "ElastiCache Redis primary endpoint"
  value       = aws_elasticache_replication_group.redis.primary_endpoint_address
}

output "redis_port" {
  description = "ElastiCache Redis port"
  value       = 6379
}

output "kafka_bootstrap_brokers" {
  description = "MSK Kafka TLS bootstrap brokers"
  value       = aws_msk_cluster.kafka.bootstrap_brokers_sasl_scram
  sensitive   = true
}

output "kafka_zookeeper_connect" {
  description = "MSK ZooKeeper connection string"
  value       = aws_msk_cluster.kafka.zookeeper_connect_string
}

output "s3_data_lake_arn" {
  description = "S3 Iceberg data lake bucket ARN"
  value       = aws_s3_bucket.data_lake.arn
}

output "s3_data_lake_bucket" {
  description = "S3 Iceberg data lake bucket name"
  value       = aws_s3_bucket.data_lake.bucket
}

output "vpc_id" {
  description = "VPC ID"
  value       = aws_vpc.main.id
}

output "private_subnet_ids" {
  description = "Private subnet IDs (for EKS, RDS, Redis)"
  value       = aws_subnet.private[*].id
}

output "public_subnet_ids" {
  description = "Public subnet IDs (for load balancers)"
  value       = aws_subnet.public[*].id
}

output "metamind_service_account_arn" {
  description = "IAM role ARN for the MetaMind Kubernetes service account (IRSA)"
  value       = aws_iam_role.metamind_sa.arn
}

output "cluster_autoscaler_role_arn" {
  description = "IAM role ARN for Cluster Autoscaler"
  value       = aws_iam_role.cluster_autoscaler.arn
}
