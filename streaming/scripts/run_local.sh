#!/usr/bin/env bash
# Ejecuta el pipeline en local (sin GCP): lee un JSONL de mensajes simulados y
# escribe la tabla Bronze en un catálogo Iceberg "hadoop" sobre un directorio.
#
# Necesita Java 11+ (Beam descarga y arranca el expansion service de IcebergIO).
# Uso: ./scripts/run_local.sh [fichero.jsonl] [directorio_warehouse]
set -euo pipefail

cd "$(dirname "$0")/.."
INPUT="${1:-tests/data/sample_messages.jsonl}"
WAREHOUSE="$(realpath -m "${2:-./.local_warehouse}")"

python main.py \
  --local_input_file="${INPUT}" \
  --catalog_type=hadoop \
  --warehouse="${WAREHOUSE}" \
  --local_dlq_path="${WAREHOUSE}/_dlq/events"

echo "Tabla Bronze escrita en ${WAREHOUSE}/bronze/events"
