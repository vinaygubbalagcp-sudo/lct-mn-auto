variable "project_id" {
  description = "GCP Project ID"
  type        = string
}

variable "region" {
  description = "Default region for GCP resources"
  type        = string
  default     = "us-central1"
}

# GCS Variables
variable "bucket_name" {
  description = "Name of the GCS bucket"
  type        = string
}

variable "storage_class" {
  description = "Storage class of the bucket"
  type        = string
  default     = "STANDARD"
}

variable "gcs_location" {
  description = "Location for the GCS bucket"
  type        = string
  default     = "US"
}

# BigQuery Variables
variable "dataset_id" {
  description = "BigQuery Dataset ID"
  type        = string
}

variable "dataset_location" {
  description = "Location of BigQuery dataset"
  type        = string
  default     = "US"
}

variable "table_name" {
  description = "BigQuery table name"
  type        = string
}

variable "table_schema" {
  description = "Schema for BigQuery table (JSON string)"
  type        = string
}


variable "composer_env_name" {
  description = "Composer environment name"
  type        = string
}

variable "composer_region" {
  description = "Region for Composer environment"
  type        = string
}

variable "composer_sa" {
  description = "Service account for Composer node config"
  type        = string
}

variable "composer_image" {
  description = "Composer image version"
  type        = string
}

variable "location" {
  type        = string
  description = "Optional location variable"
}
