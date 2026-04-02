terraform {
  required_version = ">= 1.5"
  required_providers {
    google = {
      source  = "hashicorp/google"
      version = "~> 5.0"
    }
  }
}

provider "google" {
  project = var.gcp_project
  region  = var.gcp_region
}

# Deploy Cloud Composer (managed Airflow) when orchestration_engine = "airflow"
resource "google_composer_environment" "airflow" {
  count  = var.orchestration_engine == "airflow" ? 1 : 0
  name   = "odep-airflow-${var.environment}"
  region = var.gcp_region

  config {
    software_config {
      image_version = "composer-2-airflow-2"
    }
  }
}

# Deploy DataHub on GKE when metadata_engine = "openmeta"
resource "google_container_cluster" "datahub" {
  count    = var.metadata_engine == "openmeta" ? 1 : 0
  name     = "odep-datahub-${var.environment}"
  location = var.gcp_region

  initial_node_count = 1

  node_config {
    machine_type = "e2-standard-4"
  }
}
