output "metadata_endpoint" {
  description = "Metadata service endpoint (DataHub GMS or MetaMind URL)"
  value = var.metadata_engine == "openmeta" ? (
    length(aws_ecs_cluster.datahub) > 0 ? "http://datahub-gms.${var.environment}.internal:8080" : ""
  ) : var.metamind_endpoint
}

output "orchestration_endpoint" {
  description = "Orchestration service endpoint"
  value = var.orchestration_engine == "airflow" ? (
    length(aws_mwaa_environment.airflow) > 0 ? aws_mwaa_environment.airflow[0].webserver_url : ""
  ) : ""
}

output "environment" {
  value = var.environment
}
