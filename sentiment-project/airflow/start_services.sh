#!/bin/bash

# Start all services for local development
# This script starts MLflow, Airflow webserver, and Airflow scheduler in background

set -e

export AIRFLOW_HOME=/home/felipe/sentiment-project/airflow
PROJECT_ROOT=/home/felipe/sentiment-project

echo "=== Starting Sentiment Analysis Pipeline Services ==="
echo ""

# Create log directory
mkdir -p $AIRFLOW_HOME/service_logs

# Check if services are already running
if pgrep -f "mlflow server" > /dev/null; then
    echo "MLflow is already running (PID: $(pgrep -f 'mlflow server'))"
else
    echo "Starting MLflow server..."
    nohup mlflow server --host 0.0.0.0 --port 5000 \
        --backend-store-uri sqlite:///$PROJECT_ROOT/mlflow.db \
        --default-artifact-root $PROJECT_ROOT/mlruns \
        > $AIRFLOW_HOME/service_logs/mlflow.log 2>&1 &
    echo "MLflow started (PID: $!)"
fi

sleep 2

# Start Airflow webserver
if pgrep -f "airflow webserver" > /dev/null; then
    echo "Airflow webserver is already running (PID: $(pgrep -f 'airflow webserver'))"
else
    echo "Starting Airflow webserver..."
    nohup airflow webserver -p 8080 \
        > $AIRFLOW_HOME/service_logs/webserver.log 2>&1 &
    echo "Airflow webserver started (PID: $!)"
fi

sleep 2

# Start Airflow scheduler
if pgrep -f "airflow scheduler" > /dev/null; then
    echo "Airflow scheduler is already running (PID: $(pgrep -f 'airflow scheduler'))"
else
    echo "Starting Airflow scheduler..."
    nohup airflow scheduler \
        > $AIRFLOW_HOME/service_logs/scheduler.log 2>&1 &
    echo "Airflow scheduler started (PID: $!)"
fi

echo ""
echo "=== Services Started ==="
echo ""
echo "Access UIs:"
echo "  - Airflow: http://localhost:8080 (admin/admin)"
echo "  - MLflow: http://localhost:5000"
echo ""
echo "View logs:"
echo "  - MLflow: tail -f $AIRFLOW_HOME/service_logs/mlflow.log"
echo "  - Webserver: tail -f $AIRFLOW_HOME/service_logs/webserver.log"
echo "  - Scheduler: tail -f $AIRFLOW_HOME/service_logs/scheduler.log"
echo ""
echo "To stop services:"
echo "  ./stop_services.sh"
echo ""
echo "Waiting for services to be ready..."
sleep 5

# Check service health
echo ""
echo "Service Status:"
if curl -s http://localhost:5000/health > /dev/null 2>&1; then
    echo "  ✓ MLflow: Running"
else
    echo "  ✗ MLflow: Not responding"
fi

if curl -s http://localhost:8080/health > /dev/null 2>&1; then
    echo "  ✓ Airflow Webserver: Running"
else
    echo "  ⋯ Airflow Webserver: Starting (may take 30-60 seconds)"
fi

if pgrep -f "airflow scheduler" > /dev/null; then
    echo "  ✓ Airflow Scheduler: Running"
else
    echo "  ✗ Airflow Scheduler: Not running"
fi

echo ""
echo "Ready! You can now access Airflow UI and trigger DAGs."
