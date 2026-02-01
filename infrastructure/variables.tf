# Variables for Terraform deployment
# Set these in terraform.tfvars or as environment variables

variable "synapse_sql_password" {
  description = "Password for Synapse SQL Administrator"
  type        = string
  sensitive   = true
}

variable "aad_admin_object_id" {
  description = "Object ID of Azure AD Admin for Synapse"
  type        = string
}

variable "tenant_id" {
  description = "Azure AD Tenant ID"
  type        = string
}

variable "environment" {
  description = "Environment name"
  type        = string
  default     = "production"
}

variable "location" {
  description = "Azure region"
  type        = string
  default     = "East US"
}
