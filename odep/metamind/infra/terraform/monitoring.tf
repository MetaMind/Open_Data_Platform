# ---------------------------------------------------------------------------
# CloudWatch Log Group — EKS
# ---------------------------------------------------------------------------

resource "aws_cloudwatch_log_group" "eks" {
  name              = "/aws/eks/${var.eks_cluster_name}/cluster"
  retention_in_days = 30
}

# ---------------------------------------------------------------------------
# SNS Topic — MetaMind Alerts
# ---------------------------------------------------------------------------

resource "aws_sns_topic" "alerts" {
  name         = "${var.eks_cluster_name}-alerts"
  display_name = "MetaMind Platform Alerts"
}

# ---------------------------------------------------------------------------
# CloudWatch Alarms — RDS
# ---------------------------------------------------------------------------

resource "aws_cloudwatch_metric_alarm" "rds_cpu" {
  alarm_name          = "${var.eks_cluster_name}-rds-cpu-high"
  comparison_operator = "GreaterThanThreshold"
  evaluation_periods  = 2
  metric_name         = "CPUUtilization"
  namespace           = "AWS/RDS"
  period              = 300
  statistic           = "Average"
  threshold           = 80
  alarm_description   = "RDS CPU utilization above 80% for 10 minutes"
  alarm_actions       = [aws_sns_topic.alerts.arn]
  ok_actions          = [aws_sns_topic.alerts.arn]
  dimensions = {
    DBInstanceIdentifier = aws_db_instance.postgres.identifier
  }
}

resource "aws_cloudwatch_metric_alarm" "rds_free_storage" {
  alarm_name          = "${var.eks_cluster_name}-rds-free-storage-low"
  comparison_operator = "LessThanThreshold"
  evaluation_periods  = 1
  metric_name         = "FreeStorageSpace"
  namespace           = "AWS/RDS"
  period              = 600
  statistic           = "Average"
  threshold           = 10737418240  # 10 GB in bytes
  alarm_description   = "RDS free storage below 10 GB"
  alarm_actions       = [aws_sns_topic.alerts.arn]
  dimensions = {
    DBInstanceIdentifier = aws_db_instance.postgres.identifier
  }
}

resource "aws_cloudwatch_metric_alarm" "rds_connections" {
  alarm_name          = "${var.eks_cluster_name}-rds-connections-high"
  comparison_operator = "GreaterThanThreshold"
  evaluation_periods  = 2
  metric_name         = "DatabaseConnections"
  namespace           = "AWS/RDS"
  period              = 300
  statistic           = "Average"
  threshold           = 80
  alarm_description   = "RDS connection count above 80"
  alarm_actions       = [aws_sns_topic.alerts.arn]
  dimensions = {
    DBInstanceIdentifier = aws_db_instance.postgres.identifier
  }
}

# ---------------------------------------------------------------------------
# CloudWatch Alarms — Redis
# ---------------------------------------------------------------------------

resource "aws_cloudwatch_metric_alarm" "redis_memory" {
  alarm_name          = "${var.eks_cluster_name}-redis-memory-high"
  comparison_operator = "GreaterThanThreshold"
  evaluation_periods  = 2
  metric_name         = "DatabaseMemoryUsagePercentage"
  namespace           = "AWS/ElastiCache"
  period              = 300
  statistic           = "Average"
  threshold           = 75
  alarm_description   = "Redis memory usage above 75%"
  alarm_actions       = [aws_sns_topic.alerts.arn]
  dimensions = {
    ReplicationGroupId = aws_elasticache_replication_group.redis.replication_group_id
  }
}

resource "aws_cloudwatch_metric_alarm" "redis_cpu" {
  alarm_name          = "${var.eks_cluster_name}-redis-cpu-high"
  comparison_operator = "GreaterThanThreshold"
  evaluation_periods  = 3
  metric_name         = "EngineCPUUtilization"
  namespace           = "AWS/ElastiCache"
  period              = 300
  statistic           = "Average"
  threshold           = 70
  alarm_description   = "Redis CPU above 70%"
  alarm_actions       = [aws_sns_topic.alerts.arn]
  dimensions = {
    ReplicationGroupId = aws_elasticache_replication_group.redis.replication_group_id
  }
}

# ---------------------------------------------------------------------------
# CloudWatch Alarms — MSK Kafka
# ---------------------------------------------------------------------------

resource "aws_cloudwatch_metric_alarm" "kafka_consumer_lag" {
  alarm_name          = "${var.eks_cluster_name}-kafka-consumer-lag"
  comparison_operator = "GreaterThanThreshold"
  evaluation_periods  = 3
  metric_name         = "EstimatedMaxTimeLag"
  namespace           = "AWS/Kafka"
  period              = 300
  statistic           = "Maximum"
  threshold           = 10000  # 10K messages
  alarm_description   = "Kafka consumer lag exceeds 10,000 messages"
  alarm_actions       = [aws_sns_topic.alerts.arn]
  dimensions = {
    Cluster = aws_msk_cluster.kafka.cluster_name
  }
}

resource "aws_cloudwatch_metric_alarm" "kafka_disk" {
  alarm_name          = "${var.eks_cluster_name}-kafka-disk-high"
  comparison_operator = "GreaterThanThreshold"
  evaluation_periods  = 1
  metric_name         = "KafkaDataLogsDiskUsed"
  namespace           = "AWS/Kafka"
  period              = 600
  statistic           = "Average"
  threshold           = 80
  alarm_description   = "Kafka broker disk usage above 80%"
  alarm_actions       = [aws_sns_topic.alerts.arn]
  dimensions = {
    Cluster = aws_msk_cluster.kafka.cluster_name
  }
}
