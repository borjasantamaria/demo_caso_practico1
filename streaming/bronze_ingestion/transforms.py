"""Transformaciones de mensajes Pub/Sub a registros Bronze.

Principio de la capa Bronze: no perder datos. Un payload que no es JSON válido
se guarda igualmente (``is_valid_json = False``) para poder reprocesarlo. Solo
van a la DLQ los mensajes que no se pueden representar en la tabla (cuerpo que
no es UTF-8) o que provocan un error inesperado.

Convención de metadatos del evento (todos opcionales). Se buscan primero en los
atributos de Pub/Sub y después en los campos de primer nivel del JSON:
  - event_id         identificador único del evento
  - event_type       tipo de evento (p. ej. ``order_created``)
  - event_source     sistema productor
  - event_timestamp  ISO 8601 (p. ej. ``2026-10-13T10:00:00Z``) o epoch en ms
"""

import datetime
import json
import logging
from typing import Any
from typing import Mapping
from typing import Optional

import apache_beam as beam
from apache_beam.io.gcp.pubsub import PubsubMessage
from apache_beam.utils.timestamp import Timestamp

from bronze_ingestion.schema import BronzeEvent

DLQ_TAG = 'dlq'

# Límite de Pub/Sub para el valor de un atributo: 1024 bytes.
_MAX_ATTRIBUTE_BYTES = 1024


def _first_non_empty(
    attributes: Mapping[str, str], body: Optional[dict], key: str) -> Optional[str]:
  value = attributes.get(key)
  if value:
    return value
  if body is not None:
    value = body.get(key)
    if value is not None and value != '':
      return str(value)
  return None


def parse_event_timestamp(value: Optional[str]) -> Optional[Timestamp]:
  """Interpreta ISO 8601 o epoch en milisegundos. Devuelve None si no puede."""
  if not value:
    return None
  if value.isdigit():
    return Timestamp(micros=int(value) * 1000)
  try:
    parsed = datetime.datetime.fromisoformat(value.replace('Z', '+00:00'))
  except ValueError:
    return None
  if parsed.tzinfo is None:
    # Sin zona horaria explícita se asume UTC.
    parsed = parsed.replace(tzinfo=datetime.timezone.utc)
  return Timestamp.from_utc_datetime(parsed.astimezone(datetime.timezone.utc))


def to_beam_timestamp(value: Any) -> Optional[Timestamp]:
  """Convierte el publish_time que entrega Beam (datetime) a Timestamp."""
  if value is None:
    return None
  if isinstance(value, Timestamp):
    return value
  if isinstance(value, datetime.datetime):
    if value.tzinfo is None:
      value = value.replace(tzinfo=datetime.timezone.utc)
    return Timestamp.from_utc_datetime(value.astimezone(datetime.timezone.utc))
  # protobuf Timestamp
  if hasattr(value, 'seconds') and hasattr(value, 'nanos'):
    return Timestamp(seconds=value.seconds, micros=value.nanos // 1000)
  return None


def _truncate(text: str, max_bytes: int = _MAX_ATTRIBUTE_BYTES) -> str:
  return text.encode('utf-8')[:max_bytes].decode('utf-8', errors='ignore')


class PubsubMessageToBronze(beam.DoFn):
  """Convierte un ``PubsubMessage`` en ``BronzeEvent``.

  Salida principal: ``BronzeEvent``.
  Salida ``dlq``: ``PubsubMessage`` original con atributos de error añadidos.
  """

  def __init__(self, clock=None):
    # ``clock`` permite fijar la hora de ingesta en los tests.
    self._clock = clock or Timestamp.now

  def process(
      self,
      message: PubsubMessage,
      element_timestamp=beam.DoFn.TimestampParam):
    attributes = dict(message.attributes or {})
    try:
      try:
        payload = message.data.decode('utf-8')
      except UnicodeDecodeError as e:
        yield beam.pvalue.TaggedOutput(
            DLQ_TAG, self._to_dlq(message, 'invalid_utf8', e))
        return

      try:
        body = json.loads(payload)
        is_valid_json = True
      except ValueError:
        body = None
        is_valid_json = False
      # Solo se extraen metadatos de objetos JSON (no de listas o escalares).
      body_obj = body if isinstance(body, dict) else None

      message_id = message.message_id or ''
      publish_time = (
          to_beam_timestamp(message.publish_time) or
          Timestamp.of(element_timestamp))

      yield BronzeEvent(
          event_id=(
              _first_non_empty(attributes, body_obj, 'event_id') or message_id),
          event_type=_first_non_empty(attributes, body_obj, 'event_type'),
          event_source=_first_non_empty(attributes, body_obj, 'event_source'),
          event_timestamp=parse_event_timestamp(
              _first_non_empty(attributes, body_obj, 'event_timestamp')),
          payload=payload,
          is_valid_json=is_valid_json,
          attributes=attributes,
          pubsub_message_id=message_id,
          publish_time=publish_time,
          ingestion_time=self._clock(),
      )
    except Exception as e:  # pylint: disable=broad-except
      logging.exception('Error inesperado procesando el mensaje')
      yield beam.pvalue.TaggedOutput(
          DLQ_TAG, self._to_dlq(message, 'unexpected_error', e))

  @staticmethod
  def _to_dlq(
      message: PubsubMessage, reason: str, error: Exception) -> PubsubMessage:
    attributes = dict(message.attributes or {})
    attributes.update({
        'dlq_reason': reason,
        'dlq_error': _truncate(f'{type(error).__name__}: {error}'),
        'dlq_original_message_id': message.message_id or '',
    })
    return PubsubMessage(data=message.data, attributes=attributes)
