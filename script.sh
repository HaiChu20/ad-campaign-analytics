#!/bin/bash
# Simple wrapper to run dbt commands inside docker-compose

# Run whatever command is passed to the script
docker-compose run --rm airflow "$@"
