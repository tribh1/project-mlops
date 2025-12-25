#!/bin/bash

# Quick start script for local Airflow development (without Docker)
# This script sets up Airflow with LocalExecutor and SQLite

set -e

echo "=== Local Airflow Setup for Sentiment Analysis Pipeline ==="
echo ""

# Set variables
export AIRFLOW_HOME=/home/felipe/sentiment-project/airflow
PROJECT_ROOT=/home/felipe/sentiment-project

# Check if Python 3 is installed
if ! command -v python3 &> /dev/null; then
    echo "Error: Python 3 is required but not found"
    exit 1
fi

echo "Using Python: $(python3 --version)"
echo "Airflow Home: $AIRFLOW_HOME"
echo ""

# Create necessary directories
echo "Creating directories..."
mkdir -p $AIRFLOW_HOME/dags
mkdir -p $AIRFLOW_HOME/logs
mkdir -p $AIRFLOW_HOME/plugins
mkdir -p $PROJECT_ROOT/models
mkdir -p $PROJECT_ROOT/data_clean

# Install requirements
echo ""
echo "Installing Python packages (this may take a few minutes)..."
pip3 install -q \
    apache-airflow==2.8.0 \
    pyspark==3.5.0 \
    mlflow==2.9.2 \
    matplotlib \
    seaborn \
    numpy \
    pandas

# Initialize Airflow database
echo ""
echo "Initializing Airflow database..."
export AIRFLOW__CORE__LOAD_EXAMPLES=False
airflow db init

# Create admin user
echo ""
echo "Creating Airflow admin user..."
airflow users create \
    --username admin \
    --firstname Admin \
    --lastname User \
    --role Admin \
    --email admin@example.com \
    --password admin || echo "User already exists"

# Set Airflow variables
echo ""
echo "Setting Airflow variables..."
airflow variables set mlflow_tracking_uri "http://localhost:5000"
airflow variables set mlflow_experiment_name "sentiment-analysis-pipeline"
airflow variables set data_input_path "$PROJECT_ROOT/data_raw/sentiment140/training.1600000.processed.noemoticon.csv"
airflow variables set data_output_path "$PROJECT_ROOT/data_clean"
airflow variables set model_output_path "$PROJECT_ROOT/models"
airflow variables set code_path "$PROJECT_ROOT/code"

echo ""
echo "=== Setup Complete! ==="
echo ""
echo "To start the services:"
echo ""
echo "Terminal 1 - MLflow Server:"
echo "  cd $PROJECT_ROOT"
echo "  mlflow server --host 0.0.0.0 --port 5000"
echo ""
echo "Terminal 2 - Airflow Webserver:"
echo "  export AIRFLOW_HOME=$AIRFLOW_HOME"
echo "  airflow webserver -p 8080"
echo ""
echo "Terminal 3 - Airflow Scheduler:"
echo "  export AIRFLOW_HOME=$AIRFLOW_HOME"
echo "  airflow scheduler"
echo ""
echo "Access UIs:"
echo "  - Airflow: http://localhost:8080 (admin/admin)"
echo "  - MLflow: http://localhost:5000"
echo ""
echo "To trigger the DAG:"
echo "  airflow dags trigger sentiment_training_local"
echo ""
