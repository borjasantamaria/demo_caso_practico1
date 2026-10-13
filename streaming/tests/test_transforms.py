import json

import apache_beam as beam
from apache_beam.io.gcp.pubsub import PubsubMessage
from apache_beam.testing.test_pipeline import TestPipeline
from apache_beam.testing.util import assert_that
from apache_beam.testing.util import equal_to
from apache_beam.utils.timestamp import Timestamp

from bronze_ingestion.transforms import DLQ_TAG
from bronze_ingestion.transforms import PubsubMessageToBronze
from bronze_ingestion.transforms import parse_event_timestamp

INGESTION = Timestamp(seconds=1_800_000_000)
PUBLISH = Timestamp(seconds=1_790_000_000)


def _message(data: bytes, attributes=None, message_id='m-1'):
  msg = PubsubMessage(data, attributes or {}, message_id=message_id)
  msg.publish_time = PUBLISH
  return msg


def _run(messages):
  """Ejecuta el DoFn fuera de un pipeline y separa salida principal y DLQ."""
  fn = PubsubMessageToBronze(clock=lambda: INGESTION)
  main, dlq = [], []
  for m in messages:
    for out in fn.process(m, element_timestamp=Timestamp(0)):
      if isinstance(out, beam.pvalue.TaggedOutput):
        assert out.tag == DLQ_TAG
        dlq.append(out.value)
      else:
        main.append(out)
  return main, dlq


def test_json_event_metadata_from_body():
  body = {
      'event_id': 'e-1', 'event_type': 'order_created',
      'event_source': 'shop', 'event_timestamp': '2026-10-13T10:00:00Z',
      'amount': 10}
  [row], dlq = _run([_message(json.dumps(body).encode())])
  assert dlq == []
  assert row.event_id == 'e-1'
  assert row.event_type == 'order_created'
  assert row.event_source == 'shop'
  assert row.event_timestamp == parse_event_timestamp('2026-10-13T10:00:00+00:00')
  assert json.loads(row.payload) == body
  assert row.is_valid_json is True
  assert row.pubsub_message_id == 'm-1'
  assert row.publish_time == PUBLISH
  assert row.ingestion_time == INGESTION


def test_attributes_take_precedence_over_body():
  body = {'event_id': 'body-id', 'event_type': 'body-type'}
  [row], _ = _run([_message(
      json.dumps(body).encode(),
      {'event_id': 'attr-id', 'event_type': 'attr-type'})])
  assert row.event_id == 'attr-id'
  assert row.event_type == 'attr-type'
  assert row.attributes == {'event_id': 'attr-id', 'event_type': 'attr-type'}


def test_event_id_falls_back_to_message_id():
  [row], _ = _run([_message(b'{"a": 1}', message_id='pubsub-42')])
  assert row.event_id == 'pubsub-42'
  assert row.event_type is None
  assert row.event_timestamp is None


def test_invalid_json_is_kept_in_bronze():
  [row], dlq = _run([_message(b'esto no es json')])
  assert dlq == []
  assert row.payload == 'esto no es json'
  assert row.is_valid_json is False


def test_json_array_is_valid_but_has_no_metadata():
  [row], _ = _run([_message(b'[1, 2, 3]')])
  assert row.is_valid_json is True
  assert row.event_type is None


def test_non_utf8_goes_to_dlq():
  main, [dead] = _run([_message(b'\xff\xfe\x00', {'k': 'v'})])
  assert main == []
  assert dead.data == b'\xff\xfe\x00'
  assert dead.attributes['k'] == 'v'
  assert dead.attributes['dlq_reason'] == 'invalid_utf8'
  assert dead.attributes['dlq_original_message_id'] == 'm-1'


def test_publish_time_falls_back_to_element_timestamp():
  msg = PubsubMessage(b'{}', {}, message_id='m')
  fn = PubsubMessageToBronze(clock=lambda: INGESTION)
  [row] = list(fn.process(msg, element_timestamp=Timestamp(seconds=5)))
  assert row.publish_time == Timestamp(seconds=5)


def test_parse_event_timestamp_formats():
  assert parse_event_timestamp(None) is None
  assert parse_event_timestamp('no-fecha') is None
  assert parse_event_timestamp('1700000000000') == Timestamp(seconds=1_700_000_000)
  # Sin zona horaria se asume UTC.
  assert (parse_event_timestamp('2026-10-13T10:00:00') ==
          parse_event_timestamp('2026-10-13T10:00:00Z'))
  assert (parse_event_timestamp('2026-10-13T12:00:00+02:00') ==
          parse_event_timestamp('2026-10-13T10:00:00Z'))


def test_dofn_in_pipeline_produces_schema_rows():
  with TestPipeline() as p:
    results = (
        p
        | beam.Create([_message(b'{"event_type": "x"}'), _message(b'\xff')])
        | beam.ParDo(PubsubMessageToBronze(clock=lambda: INGESTION))
        .with_outputs(DLQ_TAG, main='bronze'))
    assert_that(
        results.bronze | 'Types' >> beam.Map(lambda r: r.event_type),
        equal_to(['x']), label='bronze')
    assert_that(
        results[DLQ_TAG] | 'Reasons' >> beam.Map(
            lambda m: m.attributes['dlq_reason']),
        equal_to(['invalid_utf8']), label='dlq')
