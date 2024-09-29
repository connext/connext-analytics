#!/bin/bash

# Description: Automates the generation and serving of dbt documentation.

# Exit immediately if a command exits with a non-zero status
set -e

# Default port for serving dbt docs
DEFAULT_PORT=8080

# Use PORT environment variable if set, else default
PORT=${PORT:-$DEFAULT_PORT}

echo "=== Starting dbt Docs Generation and Serving ==="

# Navigate to the dbt project directory
# Adjust the path if your dbt project is located elsewhere
DBT_PROJECT_DIR="./connext_dbt"
cd "$DBT_PROJECT_DIR"

echo "Navigated to dbt project directory: $DBT_PROJECT_DIR"

# Install pipenv if Pipfile exists
if [ -f "Pipfile" ]; then
    echo "Pipfile found. Installing pipenv and dependencies..."
    pip install --upgrade pipenv
    pipenv install --system --deploy
    echo "Dependencies installed successfully."
else
    echo "No Pipfile found. Skipping pipenv installation."
fi

# Compile the dbt project
echo "Compiling dbt project..."
dbt compile
echo "Compilation successful."

# Generate dbt documentation
echo "Generating dbt documentation..."
dbt docs generate
echo "Documentation generated successfully."

# Serve the generated documentation
echo "Serving dbt documentation on port $PORT..."
dbt docs serve --port "$PORT" --no-browser
echo "dbt Docs are being served at http://localhost:$PORT"
