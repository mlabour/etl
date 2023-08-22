#!/bin/bash
cd c3p-model
../c3p-venv-reset.sh . && poetry update && poetry install
cd ../c3p-core
../c3p-venv-reset.sh . && poetry update && poetry install
cd ../c3p-etl
../c3p-venv-reset.sh . && poetry update && poetry install
