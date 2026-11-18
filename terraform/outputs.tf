output "resource_group_name" {
  value = azurerm_resource_group.main.name
}

output "adls_account_name" {
  value = azurerm_storage_account.datalake.name
}

output "adls_dfs_endpoint" {
  value = azurerm_storage_account.datalake.primary_dfs_endpoint
}

output "data_factory_name" {
  value = azurerm_data_factory.main.name
}

output "databricks_workspace_url" {
  value = "https://${azurerm_databricks_workspace.main.workspace_url}"
}

output "databricks_workspace_id" {
  value = azurerm_databricks_workspace.main.id
}

output "sql_server_fqdn" {
  value = azurerm_mssql_server.main.fully_qualified_domain_name
}

output "sql_database_name" {
  value = azurerm_mssql_database.marketing.name
}

output "key_vault_uri" {
  value = azurerm_key_vault.main.vault_uri
}
