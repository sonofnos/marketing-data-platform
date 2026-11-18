terraform {
  required_version = ">= 1.6"
  required_providers {
    azurerm = {
      source  = "hashicorp/azurerm"
      version = "~> 3.90"
    }
  }
}

provider "azurerm" {
  features {
    key_vault {
      purge_soft_delete_on_destroy = false
    }
  }
}

locals {
  prefix = "${var.project}-${var.environment}"
  tags   = merge(var.tags, { environment = var.environment })
}

# ── Resource Group ────────────────────────────────────────────────────────────

resource "azurerm_resource_group" "main" {
  name     = "rg-${local.prefix}"
  location = var.location
  tags     = local.tags
}

# ── ADLS Gen2 (Azure Data Lake Storage) ─────────────────────────────────────

resource "azurerm_storage_account" "datalake" {
  name                     = "${var.project}${var.environment}lake"
  resource_group_name      = azurerm_resource_group.main.name
  location                 = azurerm_resource_group.main.location
  account_tier             = "Standard"
  account_replication_type = var.adls_replication
  account_kind             = "StorageV2"
  is_hns_enabled           = true  # Hierarchical Namespace = ADLS Gen2

  blob_properties {
    versioning_enabled = true
    delete_retention_policy {
      days = 7
    }
  }

  tags = local.tags
}

# Bronze / Silver / Gold containers in ADLS Gen2
resource "azurerm_storage_data_lake_gen2_filesystem" "bronze" {
  name               = "bronze"
  storage_account_id = azurerm_storage_account.datalake.id
  properties = {
    layer = "bronze"
  }
}

resource "azurerm_storage_data_lake_gen2_filesystem" "silver" {
  name               = "silver"
  storage_account_id = azurerm_storage_account.datalake.id
  properties = {
    layer = "silver"
  }
}

resource "azurerm_storage_data_lake_gen2_filesystem" "gold" {
  name               = "gold"
  storage_account_id = azurerm_storage_account.datalake.id
  properties = {
    layer = "gold"
  }
}

resource "azurerm_storage_data_lake_gen2_filesystem" "raw" {
  name               = "raw"
  storage_account_id = azurerm_storage_account.datalake.id
  properties = {
    layer = "raw"
  }
}

# ── Azure Data Factory ────────────────────────────────────────────────────────

resource "azurerm_data_factory" "main" {
  name                = "adf-${local.prefix}"
  location            = azurerm_resource_group.main.location
  resource_group_name = azurerm_resource_group.main.name

  identity {
    type = "SystemAssigned"
  }

  tags = local.tags
}

# Grant ADF managed identity access to ADLS Gen2
resource "azurerm_role_assignment" "adf_datalake" {
  scope                = azurerm_storage_account.datalake.id
  role_definition_name = "Storage Blob Data Contributor"
  principal_id         = azurerm_data_factory.main.identity[0].principal_id
}

# ADF Linked Service to ADLS Gen2
resource "azurerm_data_factory_linked_service_data_lake_storage_gen2" "datalake" {
  name                = "ls_adls_gen2"
  data_factory_id     = azurerm_data_factory.main.id
  use_managed_identity = true
  url                 = azurerm_storage_account.datalake.primary_dfs_endpoint

  depends_on = [azurerm_role_assignment.adf_datalake]
}

# ── Azure Databricks ──────────────────────────────────────────────────────────

resource "azurerm_databricks_workspace" "main" {
  name                = "dbw-${local.prefix}"
  resource_group_name = azurerm_resource_group.main.name
  location            = azurerm_resource_group.main.location
  sku                 = var.databricks_sku

  managed_resource_group_name = "rg-${local.prefix}-databricks-managed"

  tags = local.tags
}

# ── Azure SQL (serving layer) ─────────────────────────────────────────────────

resource "azurerm_mssql_server" "main" {
  name                         = "sql-${local.prefix}"
  resource_group_name          = azurerm_resource_group.main.name
  location                     = azurerm_resource_group.main.location
  version                      = "12.0"
  administrator_login          = "sqladmin"
  administrator_login_password = random_password.sql.result
  minimum_tls_version          = "1.2"
  tags                         = local.tags
}

resource "random_password" "sql" {
  length  = 24
  special = true
}

resource "azurerm_mssql_database" "marketing" {
  name         = "marketing-platform"
  server_id    = azurerm_mssql_server.main.id
  collation    = "SQL_Latin1_General_CP1_CI_AS"
  sku_name     = "S1"
  max_size_gb  = 10
  tags         = local.tags
}

# ADF Linked Service to Azure SQL
resource "azurerm_data_factory_linked_service_azure_sql_database" "sql" {
  name            = "ls_azure_sql"
  data_factory_id = azurerm_data_factory.main.id
  connection_string = "Server=tcp:${azurerm_mssql_server.main.fully_qualified_domain_name},1433;Initial Catalog=${azurerm_mssql_database.marketing.name};Encrypt=True;TrustServerCertificate=False;Connection Timeout=30"
  use_managed_identity = true
}

# ── Key Vault (secrets) ───────────────────────────────────────────────────────

data "azurerm_client_config" "current" {}

resource "azurerm_key_vault" "main" {
  name                = "kv-${local.prefix}"
  location            = azurerm_resource_group.main.location
  resource_group_name = azurerm_resource_group.main.name
  tenant_id           = data.azurerm_client_config.current.tenant_id
  sku_name            = "standard"

  access_policy {
    tenant_id = data.azurerm_client_config.current.tenant_id
    object_id = azurerm_data_factory.main.identity[0].principal_id

    secret_permissions = ["Get", "List"]
  }

  tags = local.tags
}
