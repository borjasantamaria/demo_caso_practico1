# Warehouse del lakehouse: datos y metadatos Iceberg de todas las capas.
resource "google_storage_bucket" "lakehouse" {
  name                        = "${var.project_id}-${var.prefix}-warehouse"
  location                    = var.region
  uniform_bucket_level_access = true
  public_access_prevention    = "enforced"
  force_destroy               = var.force_destroy_buckets

  depends_on = [google_project_service.services]
}

# Ficheros temporales y de staging de Dataflow.
resource "google_storage_bucket" "dataflow" {
  name                        = "${var.project_id}-${var.prefix}-dataflow"
  location                    = var.region
  uniform_bucket_level_access = true
  public_access_prevention    = "enforced"
  force_destroy               = true

  lifecycle_rule {
    condition {
      age = 7
    }
    action {
      type = "Delete"
    }
  }

  depends_on = [google_project_service.services]
}
