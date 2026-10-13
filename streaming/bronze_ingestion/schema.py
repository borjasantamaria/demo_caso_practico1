"""Esquema de la tabla Bronze de eventos.

La capa Bronze guarda el evento tal y como llega (``payload`` en crudo) junto con
metadatos técnicos de ingesta. No se aplica ningún esquema de negocio: eso es
responsabilidad de la capa Silver.

Mapeo de tipos Beam -> Iceberg (Beam 2.77.0, ``IcebergUtils``):
  str                -> string
  bool               -> boolean
  Timestamp          -> timestamptz (logical type MicrosInstant, precisión µs)
  Mapping[str, str]  -> map<string, string>
  Optional[...]      -> columna nullable
"""

from typing import Mapping
from typing import NamedTuple
from typing import Optional

from apache_beam import coders
from apache_beam.utils.timestamp import Timestamp


class BronzeEvent(NamedTuple):
  # Identificador del evento. Si el productor no lo informa se usa el
  # message_id de Pub/Sub (permite deduplicar en Silver).
  event_id: str
  event_type: Optional[str]
  event_source: Optional[str]
  # Hora de negocio declarada por el productor (si se puede interpretar).
  event_timestamp: Optional[Timestamp]
  # Cuerpo del mensaje en crudo (UTF-8), sin modificar.
  payload: str
  is_valid_json: bool
  attributes: Mapping[str, str]
  pubsub_message_id: str
  publish_time: Timestamp
  ingestion_time: Timestamp


coders.registry.register_coder(BronzeEvent, coders.RowCoder)
