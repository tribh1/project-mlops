"""
Simplified Airflow DAG for local development (without Docker)
Runs sentiment analysis training pipeline using local Spark and MLflow.

Setup:
1. Install: pip install apache-airflow pyspark mlflow
2. Initialize: airflow db init
3. Start webserver: airflow webserver -p 8080
4. Start scheduler: airflow scheduler
5. Start MLflow: mlflow server --host 0.0.0.0 --port 5000

This version uses LocalExecutor and bash operators for easier local testing.
"""

from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator
from airflow.operators.dummy import DummyOperator
import logging
import os

# Local configuration
PROJECT_ROOT = "/home/felipe/sentiment-project"
CODE_PATH = f"{PROJECT_ROOT}/code"
DATA_INPUT = f"{PROJECT_ROOT}/data_raw/sentiment140/training.1600000.processed.noemoticon.csv"
DATA_OUTPUT = f"{PROJECT_ROOT}/data_clean"
MODEL_OUTPUT = f"{PROJECT_ROOT}/models"
MLFLOW_URI = "http://localhost:5000"
MLFLOW_EXPERIMENT = "sentiment-analysis-local"

default_args = {
    'owner': 'data-science',
    'depends_on_past': False,
    'start_date': datetime(2025, 12, 20),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=2),
}

dag = DAG(
    'sentiment_training_local',
    default_args=default_args,
    description='Local sentiment analysis training pipeline',
    schedule_interval=None,  # Manual trigger only
    catchup=False,
    tags=['ml', 'sentiment', 'local'],
)


def check_prerequisites(**context):
    """Check if all prerequisites are met"""
    import os
    import subprocess
    
    checks = []
    
    # Check data file
    if os.path.exists(DATA_INPUT):
        checks.append(("✓", f"Data file found: {DATA_INPUT}"))
    else:
        checks.append(("✗", f"Data file NOT found: {DATA_INPUT}"))
    
    # Check MLflow
    try:
        import mlflow
        checks.append(("✓", "MLflow installed"))
    except ImportError:
        checks.append(("✗", "MLflow NOT installed"))
    
    # Check PySpark
    try:
        from pyspark.sql import SparkSession
        checks.append(("✓", "PySpark installed"))
    except ImportError:
        checks.append(("✗", "PySpark NOT installed"))
    
    # Check directories
    for dir_path in [DATA_OUTPUT, MODEL_OUTPUT]:
        if os.path.exists(dir_path):
            checks.append(("✓", f"Directory exists: {dir_path}"))
        else:
            os.makedirs(dir_path, exist_ok=True)
            checks.append(("✓", f"Directory created: {dir_path}"))
    
    # Print results
    for status, message in checks:
        logging.info(f"{status} {message}")
    
    # Fail if any critical checks failed
    if any(status == "✗" for status, _ in checks):
        raise RuntimeError("Prerequisites check failed!")


def setup_mlflow(**context):
    """Setup MLflow experiment"""
    import mlflow
    
    mlflow.set_tracking_uri(MLFLOW_URI)
    
    try:
        experiment = mlflow.get_experiment_by_name(MLFLOW_EXPERIMENT)
        if experiment is None:
            experiment_id = mlflow.create_experiment(MLFLOW_EXPERIMENT)
            logging.info(f"Created MLflow experiment: {MLFLOW_EXPERIMENT} (ID: {experiment_id})")
        else:
            logging.info(f"Using existing MLflow experiment: {MLFLOW_EXPERIMENT}")
    except Exception as e:
        logging.warning(f"MLflow setup failed (is server running?): {e}")
        raise


# Tasks
start = DummyOperator(task_id='start', dag=dag)

check_prereqs = PythonOperator(
    task_id='check_prerequisites',
    python_callable=check_prerequisites,
    dag=dag,
)

setup_mlflow_task = PythonOperator(
    task_id='setup_mlflow',
    python_callable=setup_mlflow,
    dag=dag,
)

# Quick training with small sample (for testing)
train_quick = BashOperator(
    task_id='train_quick_test',
    bash_command=f"""
    cd {CODE_PATH} && \
    python sentiment_multimodel.py \
        --input {DATA_INPUT} \
        --output {MODEL_OUTPUT} \
        --mlflow-uri {MLFLOW_URI} \
        --mlflow-experiment {MLFLOW_EXPERIMENT}-quick \
        --registered-model-name twitter-sentiment-quick \
        --num-features 8192 \
        --sample-fraction 0.1 \
        --max-rows 50000
    """,
    dag=dag,
)

# Full training
train_full = BashOperator(
    task_id='train_full_dataset',
    bash_command=f"""
    cd {CODE_PATH} && \
    python sentiment_multimodel.py \
        --input {DATA_INPUT} \
        --output {MODEL_OUTPUT} \
        --mlflow-uri {MLFLOW_URI} \
        --mlflow-experiment {MLFLOW_EXPERIMENT} \
        --registered-model-name twitter-sentiment-model \
        --num-features 32768 \
        --train-ratio 0.8 \
        --val-ratio 0.1 \
        --enable-caching \
        --sample-fraction 1.0
    """,
    dag=dag,
)

# CV training with hyperparameter tuning
train_cv = BashOperator(
    task_id='train_with_cv',
    bash_command=f"""
    cd {CODE_PATH} && \
    python sentiment_multimodel.py \
        --input {DATA_INPUT} \
        --output {MODEL_OUTPUT} \
        --mlflow-uri {MLFLOW_URI} \
        --mlflow-experiment {MLFLOW_EXPERIMENT}-cv \
        --registered-model-name twitter-sentiment-cv \
        --num-features 16384 \
        --enable-cv \
        --cv-folds 2 \
        --sample-fraction 0.3
    """,
    dag=dag,
)

end = DummyOperator(task_id='end', dag=dag)

# Task dependencies
start >> check_prereqs >> setup_mlflow_task
setup_mlflow_task >> train_quick  # Test with small sample first
train_quick >> [train_full, train_cv]  # Then run both in parallel
[train_full, train_cv] >> end
