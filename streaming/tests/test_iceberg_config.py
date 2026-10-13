import pytest

from bronze_ingestion.iceberg_config import iceberg_write_config

BASE = dict(
    table='bronze.events', catalog_name='lakehouse', warehouse='gs://wh',
    project='my-project', triggering_frequency_seconds=30,
    partition_fields=['day(publish_time)'], table_properties={})


def test_biglake_streaming_config():
  config = iceberg_write_config(catalog_type='biglake', streaming=True, **BASE)
  assert config['table'] == 'bronze.events'
  assert config['triggering_frequency_seconds'] == 30
  assert config['partition_fields'] == ['day(publish_time)']
  props = config['catalog_properties']
  assert props['type'] == 'rest'
  assert props['warehouse'] == 'gs://wh'
  assert props['header.x-goog-user-project'] == 'my-project'
  assert 'table_properties' not in config


def test_batch_has_no_triggering_frequency():
  config = iceberg_write_config(catalog_type='hadoop', streaming=False, **BASE)
  assert 'triggering_frequency_seconds' not in config
  assert config['catalog_properties'] == {'type': 'hadoop', 'warehouse': 'gs://wh'}


def test_biglake_requires_project_and_gcs_warehouse():
  with pytest.raises(ValueError):
    iceberg_write_config(
        catalog_type='biglake', streaming=True, **{**BASE, 'project': None})
  with pytest.raises(ValueError):
    iceberg_write_config(
        catalog_type='biglake', streaming=True, **{**BASE, 'warehouse': '/tmp/wh'})


def test_unknown_catalog_type():
  with pytest.raises(ValueError):
    iceberg_write_config(catalog_type='glue', streaming=True, **BASE)
