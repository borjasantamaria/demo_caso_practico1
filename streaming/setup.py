"""Empaquetado para que Dataflow instale ``bronze_ingestion`` en los workers.

Se usa con ``--setup_file ./setup.py`` al lanzar el job.
"""

import setuptools

setuptools.setup(
    name='bronze-ingestion',
    version='0.1.0',
    packages=setuptools.find_packages(include=['bronze_ingestion*']),
)
