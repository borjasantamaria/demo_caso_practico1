"""Lee la tabla Bronze local (catálogo hadoop) con PyIceberg para comprobarla.

Uso: pip install pyiceberg pyarrow
     python scripts/inspect_local_table.py ./.local_warehouse
"""

import glob
import os
import sys

from pyiceberg.table import StaticTable


def main() -> None:
  warehouse = sys.argv[1] if len(sys.argv) > 1 else './.local_warehouse'
  metadata_dir = os.path.join(warehouse, 'bronze', 'events', 'metadata')
  with open(os.path.join(metadata_dir, 'version-hint.text')) as f:
    version = f.read().strip()
  metadata = os.path.join(metadata_dir, f'v{version}.metadata.json')
  table = StaticTable.from_metadata(metadata)
  print(table.schema())
  print(table.spec())
  rows = table.scan().to_arrow().to_pylist()
  for row in rows:
    print(row)
  print(f'{len(rows)} filas')


if __name__ == '__main__':
  main()
