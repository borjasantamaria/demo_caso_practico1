"""Construcción de la configuración del write Managed Iceberg de Beam.

Las claves de configuración (``table``, ``catalog_name``, ``catalog_properties``,
``triggering_frequency_seconds``, ``partition_fields``, ``table_properties``)
corresponden a ``IcebergWriteSchemaTransformProvider`` de Beam 2.77.0.

Catálogos soportados:
  - ``biglake``: BigLake metastore como Iceberg REST catalog (producción en GCP).
    Las propiedades son las que usa el test de integración oficial de Beam
    (``apache_beam/transforms/managed_iceberg_it_test.py``).
  - ``hadoop``: catálogo basado en ficheros. Útil para pruebas locales con un
    directorio como warehouse.
"""

from typing import Dict
from typing import List
from typing import Optional

BIGLAKE_REST_CATALOG_URI = 'https://biglake.googleapis.com/iceberg/v1/restcatalog'

CATALOG_TYPES = ('biglake', 'hadoop')


def catalog_properties(
    catalog_type: str, warehouse: str,
    project: Optional[str] = None) -> Dict[str, str]:
  if catalog_type == 'biglake':
    if not project:
      raise ValueError('El catálogo biglake necesita el proyecto de GCP.')
    if not warehouse.startswith('gs://'):
      raise ValueError('El warehouse de biglake debe ser una ruta gs://')
    return {
        'type': 'rest',
        'uri': BIGLAKE_REST_CATALOG_URI,
        'warehouse': warehouse,
        'header.x-goog-user-project': project,
        'rest.auth.type': 'google',
        'io-impl': 'org.apache.iceberg.gcp.gcs.GCSFileIO',
        'header.X-Iceberg-Access-Delegation': 'vended-credentials',
    }
  if catalog_type == 'hadoop':
    return {'type': 'hadoop', 'warehouse': warehouse}
  raise ValueError(
      f'catalog_type desconocido: {catalog_type!r}. Opciones: {CATALOG_TYPES}')


def iceberg_write_config(
    *,
    table: str,
    catalog_name: str,
    catalog_type: str,
    warehouse: str,
    project: Optional[str],
    streaming: bool,
    triggering_frequency_seconds: int,
    partition_fields: List[str],
    table_properties: Dict[str, str],
) -> dict:
  config = {
      'table': table,
      'catalog_name': catalog_name,
      'catalog_properties': catalog_properties(catalog_type, warehouse, project),
  }
  if streaming:
    # Cada cuánto se hace commit de un snapshot nuevo. Valores bajos -> datos
    # disponibles antes, pero más ficheros pequeños y más snapshots.
    config['triggering_frequency_seconds'] = triggering_frequency_seconds
  if partition_fields:
    # Solo se aplica cuando Beam crea la tabla (si ya existe no se modifica).
    config['partition_fields'] = partition_fields
  if table_properties:
    config['table_properties'] = table_properties
  return config
