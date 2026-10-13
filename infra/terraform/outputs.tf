output "events_topic" {
  value = google_pubsub_topic.events.id
}

output "input_subscription" {
  value = google_pubsub_subscription.events_bronze.id
}

output "dlq_topic" {
  value = google_pubsub_topic.events_dlq.id
}

output "warehouse" {
  value = "gs://${google_storage_bucket.lakehouse.name}"
}

output "dataflow_bucket" {
  value = "gs://${google_storage_bucket.dataflow.name}"
}

output "dataflow_service_account" {
  value = google_service_account.dataflow.email
}
