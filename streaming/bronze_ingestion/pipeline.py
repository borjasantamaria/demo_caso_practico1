"""Pipeline streaming: Pub/Sub -> Bronze (Apache Iceberg).

Flujo:
  ReadFromPubSub (con atributos)
    -> PubsubMessageToBronze  ──(dlq)──> WriteToPubSub(dlq_topic)
    -> Managed Iceberg Write (tabla Bronze, append)

Para pruebas locales se puede sustituir Pub/Sub por un fichero JSONL
(``--local_input_file``) y BigLake por un catálogo ``hadoop`` en un directorio.
"""

import argparse
import json
import logging
from typing import List
from typing import Optional

import apache_beam as beam
from apache_beam.io.gcp.pubsub import PubsubMessage
from apache_beam.options.pipeline_options import GoogleCloudOptions
from apache_beam.options.pipeline_options import PipelineOptions
from apache_beam.options.pipeline_options import StandardOptions
from apache_beam.utils.timestamp import Timestamp

from bronze_ingestion.iceberg_config import CATALOG_TYPES
from bronze_ingestion.iceberg_config import iceberg_write_config
from bronze_ingestion.schema import BronzeEvent
from bronze_ingestion.transforms import DLQ_TAG
from bronze_ingestion.transforms import PubsubMessageToBronze
from bronze_ingestion.transforms import parse_event_timestamp


class BronzeIngestionOptions(PipelineOptions):

  @classmethod
  def _add_argparse_args(cls, parser: argparse.ArgumentParser):
    source = parser.add_argument_group('Origen')
    source.add_argument(
        '--input_subscription',
        help='Suscripción de Pub/Sub: projects/<p>/subscriptions/<s>')
    source.add_argument(
        '--local_input_file',
        help='Solo pruebas locales: fichero JSONL con mensajes simulados '
        '({"data": ..., "attributes": {...}}) en lugar de Pub/Sub.')

    dlq = parser.add_argument_group('Dead letter queue')
    dlq.add_argument(
        '--dlq_topic',
        help='Topic de Pub/Sub para mensajes no ingeribles: '
        'projects/<p>/topics/<t>')
    dlq.add_argument(
        '--local_dlq_path',
        help='Solo pruebas locales: prefijo de fichero para la DLQ.')

    iceberg = parser.add_argument_group('Iceberg (capa Bronze)')
    iceberg.add_argument(
        '--catalog_type', choices=CATALOG_TYPES, default='biglake')
    iceberg.add_argument(
        '--catalog_name', default='lakehouse',
        help='Nombre lógico del catálogo dentro del pipeline.')
    iceberg.add_argument(
        '--warehouse', required=True,
        help='gs://<bucket> (biglake) o ruta local/gs:// (hadoop).')
    iceberg.add_argument(
        '--bronze_table', default='bronze.events',
        help='Tabla destino <namespace>.<tabla>. Se crea si no existe.')
    iceberg.add_argument(
        '--triggering_frequency_seconds', type=int, default=60,
        help='Frecuencia de commit de snapshots en streaming.')
    iceberg.add_argument(
        '--partition_fields', default='day(publish_time)',
        help='Particionado (separado por comas). Vacío = sin particionar.')
    iceberg.add_argument(
        '--iceberg_table_properties', default='{}',
        help='JSON con propiedades de tabla Iceberg para su creación.')


def parse_local_message(line: str) -> PubsubMessage:
  """Convierte una línea JSONL de prueba en un PubsubMessage."""
  record = json.loads(line)
  data = record.get('data', '')
  data = data.encode('utf-8') if isinstance(data, str) else bytes(data)
  message = PubsubMessage(
      data=data,
      attributes=record.get('attributes') or {},
      message_id=record.get('message_id'))
  # En Pub/Sub real lo asigna el servicio; aquí se simula si no viene.
  message.publish_time = (
      parse_event_timestamp(record.get('publish_time')) or Timestamp.now())
  return message


def _split_csv(value: str) -> List[str]:
  return [v.strip() for v in value.split(',') if v.strip()]


def _dlq_to_json(message: PubsubMessage) -> str:
  return json.dumps({
      'data': message.data.decode('utf-8', errors='replace'),
      'attributes': message.attributes,
  }, ensure_ascii=False)


def build_pipeline(pipeline: beam.Pipeline, options: PipelineOptions) -> None:
  opts = options.view_as(BronzeIngestionOptions)
  streaming = options.view_as(StandardOptions).streaming
  project: Optional[str] = options.view_as(GoogleCloudOptions).project

  if bool(opts.input_subscription) == bool(opts.local_input_file):
    raise ValueError(
        'Indica exactamente uno de --input_subscription o --local_input_file')

  if opts.input_subscription:
    messages = pipeline | 'ReadPubSub' >> beam.io.ReadFromPubSub(
        subscription=opts.input_subscription, with_attributes=True)
  else:
    messages = (
        pipeline
        | 'ReadLocalFile' >> beam.io.ReadFromText(opts.local_input_file)
        | 'ParseLocalMessage' >> beam.Map(parse_local_message))

  results = (
      messages
      | 'ToBronzeRecord' >> beam.ParDo(PubsubMessageToBronze())
      .with_output_types(BronzeEvent)
      .with_outputs(DLQ_TAG, main='bronze'))

  _ = results.bronze | 'WriteBronzeIceberg' >> beam.managed.Write(
      beam.managed.ICEBERG,
      config=iceberg_write_config(
          table=opts.bronze_table,
          catalog_name=opts.catalog_name,
          catalog_type=opts.catalog_type,
          warehouse=opts.warehouse,
          project=project,
          streaming=streaming,
          triggering_frequency_seconds=opts.triggering_frequency_seconds,
          partition_fields=_split_csv(opts.partition_fields),
          table_properties=json.loads(opts.iceberg_table_properties),
      ))

  dlq = results[DLQ_TAG]
  if opts.dlq_topic:
    _ = dlq | 'WriteDLQ' >> beam.io.WriteToPubSub(
        opts.dlq_topic, with_attributes=True)
  elif opts.local_dlq_path:
    _ = (
        dlq
        | 'DLQToJson' >> beam.Map(_dlq_to_json)
        | 'WriteLocalDLQ' >> beam.io.WriteToText(
            opts.local_dlq_path, file_name_suffix='.jsonl'))
  else:
    logging.warning(
        'Sin --dlq_topic: los mensajes no ingeribles solo se registran en log.')
    _ = dlq | 'LogDLQ' >> beam.Map(
        lambda m: logging.error('Mensaje a DLQ: %s', m.attributes))


def run(argv=None) -> None:
  options = PipelineOptions(argv, save_main_session=False)
  with beam.Pipeline(options=options) as pipeline:
    build_pipeline(pipeline, options)

