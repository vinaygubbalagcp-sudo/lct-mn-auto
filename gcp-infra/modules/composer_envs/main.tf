resource "google_composer_environment" "composer_env" {
  provider = google

  name   = var.composer_env_name
  region = var.composer_region

  config {
    software_config {
      image_version = var.composer_image
    }

    node_config {
      service_account = var.composer_sa
    }
  }
}
