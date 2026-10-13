# Catálogo Iceberg REST de BigLake metastore sobre el bucket del warehouse.
# Para CATALOG_TYPE_GCS_BUCKET el nombre del catálogo debe ser el del bucket.
resource "google_biglake_iceberg_catalog" "lakehouse" {
  name            = google_storage_bucket.lakehouse.name
  catalog_type    = "CATALOG_TYPE_GCS_BUCKET"
  credential_mode = "CREDENTIAL_MODE_VENDED_CREDENTIALS"

  depends_on = [google_project_service.services]
}

# Con credential vending, BigLake genera credenciales acotadas a partir de su
# propia cuenta de servicio, que necesita acceso al bucket. Se usa el rol del
# ejemplo oficial del provider.
resource "google_storage_bucket_iam_member" "catalog_vending_sa" {
  bucket = google_storage_bucket.lakehouse.name
  role   = "roles/storage.admin"
  member = "serviceAccount:${google_biglake_iceberg_catalog.lakehouse.biglake_service_account}"
}
