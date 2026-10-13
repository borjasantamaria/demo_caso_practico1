"""Publica eventos de prueba en el topic de Pub/Sub.

Uso:
  python scripts/publish_test_events.py --project mi-proyecto \
      --topic lakehouse-events --count 20
"""

import argparse
import datetime
import json
import random
import uuid

from google.cloud import pubsub_v1

EVENT_TYPES = ['order_created', 'order_paid', 'order_shipped']


def build_event(i: int) -> dict:
  return {
      'event_id': str(uuid.uuid4()),
      'event_type': random.choice(EVENT_TYPES),
      'event_source': 'test-publisher',
      'event_timestamp': datetime.datetime.now(datetime.timezone.utc).isoformat(),
      'order_id': i,
      'amount': round(random.uniform(5, 500), 2),
  }


def main() -> None:
  parser = argparse.ArgumentParser()
  parser.add_argument('--project', required=True)
  parser.add_argument('--topic', default='lakehouse-events')
  parser.add_argument('--count', type=int, default=10)
  args = parser.parse_args()

  publisher = pubsub_v1.PublisherClient()
  topic_path = publisher.topic_path(args.project, args.topic)
  futures = []
  for i in range(args.count):
    event = build_event(i)
    futures.append(publisher.publish(
        topic_path,
        json.dumps(event).encode('utf-8'),
        # Metadatos también como atributos: el pipeline los lee de aquí
        # primero, sin depender del formato del payload.
        event_type=event['event_type'],
        event_id=event['event_id'],
        schema_version='1'))
  for f in futures:
    f.result()
  print(f'Publicados {args.count} eventos en {topic_path}')


if __name__ == '__main__':
  main()
