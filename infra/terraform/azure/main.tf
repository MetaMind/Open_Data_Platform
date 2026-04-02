terraform {
  required_version = ">= 1.5"
  required_providers {
    azurerm = {
      source  = "hashicorp/azurerm"
      version = "~> 3.0"
    }
  }
}

provider "azurerm" {
  features {}
}

resource "azurerm_resource_group" "odep" {
  name     = var.resource_group_name
  location = var.azure_location
}

# Deploy AKS cluster for DataHub when metadata_engine = "openmeta"
resource "azurerm_kubernetes_cluster" "datahub" {
  count               = var.metadata_engine == "openmeta" ? 1 : 0
  name                = "odep-datahub-${var.environment}"
  location            = azurerm_resource_group.odep.location
  resource_group_name = azurerm_resource_group.odep.name
  dns_prefix          = "odep-datahub-${var.environment}"

  default_node_pool {
    name       = "default"
    node_count = 2
    vm_size    = "Standard_D4s_v3"
  }

  identity {
    type = "SystemAssigned"
  }
}

# Store MetaMind API key in Key Vault when metadata_engine = "metamind"
resource "azurerm_key_vault" "metamind" {
  count               = var.metadata_engine == "metamind" ? 1 : 0
  name                = "odep-kv-${var.environment}"
  location            = azurerm_resource_group.odep.location
  resource_group_name = azurerm_resource_group.odep.name
  tenant_id           = data.azurerm_client_config.current.tenant_id
  sku_name            = "standard"
}

data "azurerm_client_config" "current" {}
