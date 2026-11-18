variable "location" {
  description = "Azure region"
  type        = string
  default     = "West Europe"
}

variable "environment" {
  description = "Deployment environment (dev/staging/prod)"
  type        = string
  default     = "dev"
  validation {
    condition     = contains(["dev", "staging", "prod"], var.environment)
    error_message = "environment must be dev, staging, or prod"
  }
}

variable "project" {
  description = "Project identifier used in resource names"
  type        = string
  default     = "mktdp"
}

variable "databricks_sku" {
  description = "Databricks SKU (trial/standard/premium)"
  type        = string
  default     = "standard"
}

variable "adls_replication" {
  description = "Storage account replication type"
  type        = string
  default     = "LRS"
}

variable "tags" {
  description = "Common resource tags"
  type        = map(string)
  default = {
    project   = "marketing-data-platform"
    managed   = "terraform"
  }
}
