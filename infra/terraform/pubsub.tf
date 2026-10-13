# Topic donde los productores publican los eventos.
resource "google_pubsub_topic" "events" {
  name       = "${var.prefix}-events"
  depends_on = [google_project_service.services]
}

# Suscripción que consume el job de Dataflow.
resource "google_pubsub_subscription" "events_bronze" {
  name                       = "${var.prefix}-events-bronze-ingestion"
  topic                      = google_pubsub_topic.events.id
  ack_deadline_seconds       = 60
  message_retention_duration = var.pubsub_message_retention
  retain_acked_messages      = true # permite seek/replay sobre mensajes ya procesados

  expiration_policy {
    ttl = "" # nunca expira
  }
}

# Dead letter queue: mensajes que el pipeline no puede escribir en Bronze.
resource "google_pubsub_topic" "events_dlq" {
  name       = "${var.prefix}-events-dlq"
  depends_on = [google_project_service.services]
}

resource "google_pubsub_subscription" "events_dlq" {
  name                       = "${var.prefix}-events-dlq-inspection"
  topic                      = google_pubsub_topic.events_dlq.id
  message_retention_duration = var.pubsub_message_retention

  expiration_policy {
    ttl = ""
  }
}
