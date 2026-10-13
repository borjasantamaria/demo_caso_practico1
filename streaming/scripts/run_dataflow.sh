#!/usr/bin/env bash
# Lanza el job streaming de ingesta Bronze en Dataflow.
# Requiere haber aplicado infra/terraform y tener credenciales de gcloud.
#
# Uso: PROJECT_ID=mi-proyecto ./scripts/run_dataflow.sh
set -euo pipefail

: "${PROJECT_ID:?Define PROJECT_ID}"
REGION="${REGION:-europe-west1}"
PREFIX="${PREFIX:-lakehouse}"
JOB_NAME="${JOB_NAME:-${PREFIX}-bronze-events-ingestion}"

WAREHOUSE="gs://${PROJECT_ID}-${PREFIX}-warehouse"
DATAFLOW_BUCKET="gs://${PROJECT_ID}-${PREFIX}-dataflow"
SERVICE_ACCOUNT="${PREFIX}-dataflow@${PROJECT_ID}.iam.gserviceaccount.com"

cd "$(dirname "$0")/.."

python main.py \
  --runner=DataflowRunner \
  --project="${PROJECT_ID}" \
  --region="${REGION}" \
  --job_name="${JOB_NAME}" \
  --streaming \
  --temp_location="${DATAFLOW_BUCKET}/temp" \
  --staging_location="${DATAFLOW_BUCKET}/staging" \
  --service_account_email="${SERVICE_ACCOUNT}" \
  --setup_file=./setup.py \
  --max_num_workers="${MAX_NUM_WORKERS:-3}" \
  --input_subscription="projects/${PROJECT_ID}/subscriptions/${PREFIX}-events-bronze-ingestion" \
  --dlq_topic="projects/${PROJECT_ID}/topics/${PREFIX}-events-dlq" \
  --catalog_type=biglake \
  --catalog_name=lakehouse \
  --warehouse="${WAREHOUSE}" \
  --bronze_table=bronze.events \
  --triggering_frequency_seconds="${TRIGGERING_FREQUENCY_SECONDS:-60}" \
  "$@"
