#!/bin/bash

# Setup virtual environment
cd c3p-model
../c3p-venv-reset.sh . && poetry update && poetry install
cd ../c3p-core
../c3p-venv-reset.sh . && poetry update && poetry install
cd ../c3p-etl
../c3p-venv-reset.sh . && poetry update && poetry install

# Cleanup the data
mkdir -p ./data/source
mkdir -p ./data/source.bak
mkdir -p ./data/target

# Run the ETL pipeline
cd ../c3p-etl
poetry run scripts/run_etl.py


