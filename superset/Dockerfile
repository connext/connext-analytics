# Use the official Apache Superset image as the base
FROM apache/superset:latest

# Set environment variables
ENV SUPERSET_HOME=/app/superset
ENV PYTHONPATH=/app/pythonpath

# Copy Superset configuration to the PYTHONPATH directory
COPY superset/superset_config.py ${PYTHONPATH}/superset_config.py

# Copy the requirements.txt file
COPY superset/requirements.txt ${SUPERSET_HOME}/requirements.txt

# Upgrade pip to the latest version
RUN pip install --upgrade pip

# Install the required packages
RUN pip install --no-cache-dir -r ${SUPERSET_HOME}/requirements.txt

# Expose the port Superset runs on
EXPOSE 8088

# Initialize Superset
CMD ["gunicorn", "-b", "0.0.0.0:8088", "--timeout", "120", "superset.app:create_app()"]