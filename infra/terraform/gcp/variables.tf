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

variable "gcp_project" {
  description = "GCP project ID"
  type        = string
}

variable "gcp_region" {
  description = "GCP region"
  type        = string
  default     = "us-central1"
}

variable "metamind_endpoint" {
  description = "MetaMind API endpoint (required when metadata_engine = metamind)"
  type        = string
  default     = ""
}
