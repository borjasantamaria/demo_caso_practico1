variable "project_id" {
  description = "ID del proyecto de GCP."
  type        = string
}

variable "region" {
  description = "Región para Dataflow, buckets y catálogo."
  type        = string
  default     = "europe-west1"
}

variable "prefix" {
  description = "Prefijo para nombrar los recursos (los buckets deben ser únicos a nivel global)."
  type        = string
  default     = "lakehouse"
}

variable "pubsub_message_retention" {
  description = "Retención de mensajes en la suscripción (permite reprocesar)."
  type        = string
  default     = "604800s" # 7 días
}

variable "force_destroy_buckets" {
  description = "Permite borrar buckets con datos en terraform destroy (solo entornos de demo)."
  type        = bool
  default     = false
}
