#!/bin/bash

# Fix Airflow directory permissions and structure
# Run this if you encounter directory creation errors

set -e

echo "=== Fixing Airflow Directory Structure ==="

# Get Airflow UID
export AIRFLOW_UID=$(id -u)
echo "Using AIRFLOW_UID: $AIRFLOW_UID"

# Stop containers if running
echo "Stopping containers..."
docker-compose down 2>/dev/null || true

# Create all necessary directories on host
echo "Creating host directories..."
mkdir -p dags logs plugins config
mkdir -p logs/scheduler
mkdir -p logs/dag_processor_manager
mkdir -p ../models ../data_clean
mkdir -p ../data_raw/sentiment140

# Set proper permissions
echo "Setting permissions..."
chmod -R 777 logs  # More permissive for Docker
chmod -R 755 dags plugins config

# Create directories inside containers
echo "Creating container directories..."
docker-compose run --rm airflow-init bash -c "
  mkdir -p /opt/airflow/logs/scheduler
  mkdir -p /opt/airflow/logs/dag_processor_manager  
  mkdir -p /opt/airflow/data/raw/sentiment140
  mkdir -p /opt/airflow/data/clean
  mkdir -p /opt/airflow/models
  chmod -R 755 /opt/airflow/logs
  echo 'Directories created successfully'
" || echo "Note: Some errors are expected on first run"

# Reinitialize database
echo "Reinitializing Airflow..."
docker-compose up airflow-init

echo ""
echo "=== Fix Complete ==="
echo "Now run: ./setup.sh to complete setup"
echo "Or run: docker-compose up -d to start services"
