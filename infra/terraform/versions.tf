terraform {
  required_version = ">= 1.5"

  required_providers {
    google = {
      source = "hashicorp/google"
      # Validado con 8.6.0 (incluye google_biglake_iceberg_catalog).
      version = ">= 8.6.0, < 9.0.0"
    }
  }
}

provider "google" {
  project = var.project_id
  region  = var.region
}
