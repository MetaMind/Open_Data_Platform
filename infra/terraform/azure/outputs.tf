output "metadata_endpoint" {
  description = "Metadata service endpoint (DataHub GMS or MetaMind URL)"
  value = var.metadata_engine == "openmeta" ? (
    length(azurerm_kubernetes_cluster.datahub) > 0 ? "http://datahub-gms.${var.environment}.internal:8080" : ""
  ) : var.metamind_endpoint
}

output "environment" {
  value = var.environment
}

output "resource_group" {
  value = azurerm_resource_group.odep.name
}
