# Enterprise Cloud Migration - Infrastructure as Code
# Terraform configuration for Azure Synapse Analytics deployment
# Author: Avinash Chinnabattuni

terraform {
  required_providers {
    azurerm = {
      source  = "hashicorp/azurerm"
      version = "~> 3.0"
    }
  }
  required_version = ">= 1.0"
}

provider "azurerm" {
  features {}
}

# Resource Group
resource "azurerm_resource_group" "data_platform" {
  name     = "rg-dataplatform-prod"
  location = "East US"
  
  tags = {
    Environment = "Production"
    Project     = "Enterprise-Migration"
    Owner       = "Data Engineering Team"
    CostCenter  = "IT-Analytics"
  }
}

# Storage Account (Data Lake Gen2)
resource "azurerm_storage_account" "datalake" {
  name                     = "sadataplatformprod001"
  resource_group_name      = azurerm_resource_group.data_platform.name
  location                 = azurerm_resource_group.data_platform.location
  account_tier             = "Standard"
  account_replication_type = "GRS"  # Geo-redundant for DR
  account_kind             = "StorageV2"
  is_hns_enabled           = "true"  # Enable hierarchical namespace for Data Lake
  
  blob_properties {
    delete_retention_policy {
      days = 7
    }
    container_delete_retention_policy {
      days = 7
    }
  }
  
  network_rules {
    default_action             = "Deny"
    virtual_network_subnet_ids = [azurerm_subnet.data_subnet.id]
    bypass                     = ["AzureServices"]
  }
  
  tags = azurerm_resource_group.data_platform.tags
}

# Storage Containers (Bronze/Silver/Gold)
resource "azurerm_storage_container" "bronze" {
  name                  = "bronze"
  storage_account_name  = azurerm_storage_account.datalake.name
  container_access_type = "private"
}

resource "azurerm_storage_container" "silver" {
  name                  = "silver"
  storage_account_name  = azurerm_storage_account.datalake.name
  container_access_type = "private"
}

resource "azurerm_storage_container" "gold" {
  name                  = "gold"
  storage_account_name  = azurerm_storage_account.datalake.name
  container_access_type = "private"
}

# Virtual Network
resource "azurerm_virtual_network" "data_vnet" {
  name                = "vnet-dataplatform-prod"
  address_space       = ["10.0.0.0/16"]
  location            = azurerm_resource_group.data_platform.location
  resource_group_name = azurerm_resource_group.data_platform.name
  
  tags = azurerm_resource_group.data_platform.tags
}

resource "azurerm_subnet" "data_subnet" {
  name                 = "snet-data-prod"
  resource_group_name  = azurerm_resource_group.data_platform.name
  virtual_network_name = azurerm_virtual_network.data_vnet.name
  address_prefixes     = ["10.0.1.0/24"]
  
  service_endpoints = ["Microsoft.Storage", "Microsoft.Sql", "Microsoft.KeyVault"]
}

# Azure Synapse Workspace
resource "azurerm_synapse_workspace" "synapse" {
  name                                 = "synapse-dataplatform-prod"
  resource_group_name                  = azurerm_resource_group.data_platform.name
  location                             = azurerm_resource_group.data_platform.location
  storage_data_lake_gen2_filesystem_id = azurerm_storage_container.bronze.id
  sql_administrator_login              = "sqladmin"
  sql_administrator_login_password     = var.synapse_sql_password
  
  aad_admin {
    login     = "AzureAD Admin"
    object_id = var.aad_admin_object_id
    tenant_id = var.tenant_id
  }
  
  sql_identity_control_enabled = true
  
  tags = azurerm_resource_group.data_platform.tags
}

# Synapse SQL Pool (Dedicated)
resource "azurerm_synapse_sql_pool" "dedicated_pool" {
  name                 = "sqlpoolprod"
  synapse_workspace_id = azurerm_synapse_workspace.synapse.id
  sku_name             = "DW100c"  # Start small, scale up
  create_mode          = "Default"
  
  tags = azurerm_resource_group.data_platform.tags
}

# Synapse Spark Pool (Serverless)
resource "azurerm_synapse_spark_pool" "spark_pool" {
  name                 = "sparkpoolprod"
  synapse_workspace_id = azurerm_synapse_workspace.synapse.id
  node_size_family     = "MemoryOptimized"
  node_size            = "Small"
  cache_size           = 100
  
  auto_scale {
    max_node_count = 10
    min_node_count = 3
  }
  
  auto_pause {
    delay_in_minutes = 15
  }
  
  tags = azurerm_resource_group.data_platform.tags
}

# Azure Data Factory
resource "azurerm_data_factory" "adf" {
  name                = "adf-dataplatform-prod"
  location            = azurerm_resource_group.data_platform.location
  resource_group_name = azurerm_resource_group.data_platform.name
  
  identity {
    type = "SystemAssigned"
  }
  
  tags = azurerm_resource_group.data_platform.tags
}

# Key Vault for Secrets
resource "azurerm_key_vault" "data_vault" {
  name                       = "kv-dataplatform-prod"
  location                   = azurerm_resource_group.data_platform.location
  resource_group_name        = azurerm_resource_group.data_platform.name
  tenant_id                  = var.tenant_id
  sku_name                   = "standard"
  soft_delete_retention_days = 7
  purge_protection_enabled   = true
  
  network_acls {
    default_action = "Deny"
    bypass         = "AzureServices"
  }
  
  tags = azurerm_resource_group.data_platform.tags
}

# Monitoring
resource "azurerm_log_analytics_workspace" "monitoring" {
  name                = "log-dataplatform-prod"
  location            = azurerm_resource_group.data_platform.location
  resource_group_name = azurerm_resource_group.data_platform.name
  sku                 = "PerGB2018"
  retention_in_days   = 30
  
  tags = azurerm_resource_group.data_platform.tags
}

# Outputs
output "synapse_workspace_name" {
  value = azurerm_synapse_workspace.synapse.name
}

output "storage_account_name" {
  value = azurerm_storage_account.datalake.name
}

output "resource_group_name" {
  value = azurerm_resource_group.data_platform.name
}
