output "metadata_endpoint" {
  description = "Metadata service endpoint (DataHub GMS or MetaMind URL)"
  value = var.metadata_engine == "openmeta" ? (
    length(google_container_cluster.datahub) > 0 ? "http://datahub-gms.${var.environment}.internal:8080" : ""
  ) : var.metamind_endpoint
}

output "orchestration_endpoint" {
  description = "Cloud Composer Airflow web server URL"
  value = var.orchestration_engine == "airflow" ? (
    length(google_composer_environment.airflow) > 0 ? google_composer_environment.airflow[0].config[0].airflow_uri : ""
  ) : ""
}

output "environment" {
  value = var.environment
}
