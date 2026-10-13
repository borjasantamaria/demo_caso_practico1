# Cuenta de servicio de los workers de Dataflow.
resource "google_service_account" "dataflow" {
  account_id   = "${var.prefix}-dataflow"
  display_name = "Dataflow - ingesta streaming Bronze"
}

locals {
  dataflow_sa = "serviceAccount:${google_service_account.dataflow.email}"
}

resource "google_project_iam_member" "dataflow_worker" {
  project = var.project_id
  role    = "roles/dataflow.worker"
  member  = local.dataflow_sa
}

# El catálogo REST se invoca con la cabecera x-goog-user-project.
resource "google_project_iam_member" "dataflow_service_usage" {
  project = var.project_id
  role    = "roles/serviceusage.serviceUsageConsumer"
  member  = local.dataflow_sa
}

resource "google_pubsub_subscription_iam_member" "dataflow_subscriber" {
  subscription = google_pubsub_subscription.events_bronze.name
  role         = "roles/pubsub.subscriber"
  member       = local.dataflow_sa
}

resource "google_pubsub_subscription_iam_member" "dataflow_subscription_viewer" {
  subscription = google_pubsub_subscription.events_bronze.name
  role         = "roles/pubsub.viewer"
  member       = local.dataflow_sa
}

resource "google_pubsub_topic_iam_member" "dataflow_dlq_publisher" {
  topic  = google_pubsub_topic.events_dlq.name
  role   = "roles/pubsub.publisher"
  member = local.dataflow_sa
}

resource "google_storage_bucket_iam_member" "dataflow_temp" {
  bucket = google_storage_bucket.dataflow.name
  role   = "roles/storage.objectAdmin"
  member = local.dataflow_sa
}

# Crear namespaces/tablas y hacer commits en el catálogo Iceberg.
resource "google_biglake_iceberg_catalog_iam_member" "dataflow_catalog_editor" {
  name   = google_biglake_iceberg_catalog.lakehouse.name
  role   = "roles/biglake.editor"
  member = local.dataflow_sa
}
