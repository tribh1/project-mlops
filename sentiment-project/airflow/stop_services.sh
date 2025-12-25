#!/bin/bash

# Stop all services (MLflow, Airflow)

echo "=== Stopping Services ==="
echo ""

# Stop Airflow scheduler
if pgrep -f "airflow scheduler" > /dev/null; then
    echo "Stopping Airflow scheduler..."
    pkill -f "airflow scheduler"
    echo "  ✓ Airflow scheduler stopped"
else
    echo "  - Airflow scheduler not running"
fi

# Stop Airflow webserver
if pgrep -f "airflow webserver" > /dev/null; then
    echo "Stopping Airflow webserver..."
    pkill -f "airflow webserver"
    # Also kill gunicorn workers
    pkill -f "gunicorn: master"
    pkill -f "gunicorn: worker"
    echo "  ✓ Airflow webserver stopped"
else
    echo "  - Airflow webserver not running"
fi

# Stop MLflow
if pgrep -f "mlflow server" > /dev/null; then
    echo "Stopping MLflow server..."
    pkill -f "mlflow server"
    echo "  ✓ MLflow server stopped"
else
    echo "  - MLflow server not running"
fi

echo ""
echo "=== All Services Stopped ==="
