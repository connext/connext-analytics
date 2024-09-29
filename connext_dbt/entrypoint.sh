#!/bin/bash
set -e

# Ensure the PORT variable is set for Google Cloud Run
if [ -z "$PORT" ]; then
  echo "Error: PORT environment variable is not set."
  exit 1
fi

# Install dbt dependencies
pipenv run dbt deps

# Generate dbt documentation
pipenv run dbt docs generate

# Serve the documentation on the correct port
pipenv run dbt docs serve --port $PORT --no-browser --host 0.0.0.0