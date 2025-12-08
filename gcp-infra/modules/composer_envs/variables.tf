variable "composer_env_name" {
  description = "Composer environment name"
  type        = string
}

variable "composer_region" {
  description = "Region for Composer environment"
  type        = string
}

variable "composer_sa" {
  description = "Service account used by Composer workers"
  type        = string
}

variable "composer_image" {
  description = "Image version for Composer"
  type        = string
}
