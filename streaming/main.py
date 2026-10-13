"""Punto de entrada del job de ingesta Bronze (local o Dataflow)."""

import logging

from bronze_ingestion.pipeline import run

if __name__ == '__main__':
  logging.getLogger().setLevel(logging.INFO)
  run()
