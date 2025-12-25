#!/bin/bash

# Setup script for Airflow with Spark and MLflow
# Run this script to initialize the Airflow environment

set -e

echo "=== Airflow Environment Setup ==="

# Set Airflow UID
export AIRFLOW_UID=$(id -u)
echo "AIRFLOW_UID=$AIRFLOW_UID" > .env

# Create necessary directories
echo "Creating directories..."
mkdir -p dags logs plugins config
mkdir -p logs/scheduler
mkdir -p ../models ../data_clean
mkdir -p ../data_raw/sentiment140

# Set permissions
echo "Setting permissions..."
chmod -R 755 dags logs plugins config
chown -R $AIRFLOW_UID:$AIRFLOW_UID dags logs plugins config 2>/dev/null || true
chmod 755 ../code/sentiment_multimodel.py

# Initialize Airflow database
echo "Initializing Airflow database..."
docker-compose up airflow-init

# Start services
echo "Starting Airflow services..."
docker-compose up -d

# Wait for services to be healthy
echo "Waiting for services to be ready..."
sleep 30

# Check service status
echo "Checking service status..."
docker-compose ps

echo ""
echo "=== Setup Complete ==="
echo ""
echo "Services:"
echo "  - Airflow UI: http://localhost:8080"
echo "    Username: airflow"
echo "    Password: airflow"
echo "  - MLflow UI: http://localhost:5000"
echo "  - Flower (Celery): http://localhost:5555 (use 'docker-compose --profile flower up' to enable)"
echo ""
echo "To view logs:"
echo "  docker-compose logs -f airflow-scheduler"
echo "  docker-compose logs -f airflow-worker"
echo "  docker-compose logs -f mlflow"
echo ""
echo "To stop services:"
echo "  docker-compose down"
echo ""
echo "To restart services:"
echo "  docker-compose restart"
echo ""

# Create Spark connection in Airflow
echo "Creating Spark connection in Airflow..."
docker-compose exec -T airflow-webserver airflow connections add 'spark_default' \
    --conn-type 'spark' \
    --conn-host 'local' \
    --conn-extra '{"deploy-mode": "client"}' || echo "Connection already exists"

# Set Airflow variables
echo "Setting Airflow variables..."
docker-compose exec -T airflow-webserver airflow variables set mlflow_tracking_uri "http://mlflow:5000"
docker-compose exec -T airflow-webserver airflow variables set mlflow_experiment_name "sentiment-analysis-pipeline"
docker-compose exec -T airflow-webserver airflow variables set data_input_path "/opt/airflow/data/raw/sentiment140/training.1600000.processed.noemoticon.csv"
docker-compose exec -T airflow-webserver airflow variables set data_output_path "/opt/airflow/data/clean"
docker-compose exec -T airflow-webserver airflow variables set model_output_path "/opt/airflow/models"
docker-compose exec -T airflow-webserver airflow variables set code_path "/opt/airflow/code"

echo ""
echo "=== Configuration Complete ==="
echo "You can now access Airflow UI and trigger the DAG 'sentiment_ml_training_pipeline'"
