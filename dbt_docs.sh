#!/bin/bash

# Exit immediately if a command exits with a non-zero status
set -e

echo "=== Starting dbt Docs Generation and Serving ==="

# Retrieve profiles.yml using the separate Python script
pipenv run python -m connext_dbt.config.get_profile
echo "profiles.yml has been written to /app/.dbt/profiles.yml"

# Navigate to the dbt project directory
DBT_PROJECT_DIR="./connext_dbt"
cd "$DBT_PROJECT_DIR"
echo "Navigated to dbt project directory: $DBT_PROJECT_DIR"

# Run dbt docs generate using pipenv and the correct profiles directory
echo "Generating dbt documentation..."
pipenv run dbt docs generate --profiles-dir /app/.dbt
echo "Documentation generated successfully."

# Serve the generated documentation using pipenv
echo "Serving dbt documentation on port $PORT..."
pipenv run dbt docs serve --port "$PORT" --profiles-dir /app/.dbt --no-browser