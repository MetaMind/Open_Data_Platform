variable "metadata_engine" {
  description = "Metadata engine to deploy: 'openmeta' or 'metamind'"
  type        = string
  default     = "openmeta"

  validation {
    condition     = contains(["openmeta", "metamind"], var.metadata_engine)
    error_message = "metadata_engine must be one of: openmeta, metamind"
  }
}

variable "orchestration_engine" {
  description = "Orchestration engine: 'airflow', 'dagster', 'prefect', 'temporal'"
  type        = string
  default     = "airflow"

  validation {
    condition     = contains(["airflow", "dagster", "prefect", "temporal"], var.orchestration_engine)
    error_message = "orchestration_engine must be one of: airflow, dagster, prefect, temporal"
  }
}

variable "execution_engine" {
  description = "Default execution engine"
  type        = string
  default     = "spark"
}

variable "environment" {
  description = "Deployment environment: dev, staging, prod"
  type        = string
  default     = "dev"
}

variable "azure_location" {
  description = "Azure region"
  type        = string
  default     = "eastus"
}

variable "resource_group_name" {
  description = "Azure resource group name"
  type        = string
  default     = "odep-rg"
}

variable "metamind_endpoint" {
  description = "MetaMind API endpoint (required when metadata_engine = metamind)"
  type        = string
  default     = ""
}
