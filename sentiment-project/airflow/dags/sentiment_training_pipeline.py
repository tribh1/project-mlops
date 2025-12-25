"""
Airflow DAG for Sentiment Analysis ML Training Pipeline with MLflow
Orchestrates data preparation, model training, and experiment tracking.

This DAG:
1. Validates input data
2. Prepares training dataset
3. Trains multiple models (LogisticRegression, RandomForest, GBT, LinearSVC)
4. Logs experiments to MLflow
5. Selects and registers the best model
6. Sends notifications on completion
"""

from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator
from airflow.providers.google.cloud.operators.dataproc import DataprocSubmitPySparkJobOperator
from airflow.operators.dummy import DummyOperator
from airflow.utils.task_group import TaskGroup
from airflow.models import Variable
import logging
import os

# Configuration from Airflow Variables
MLFLOW_TRACKING_URI = Variable.get("mlflow_tracking_uri", default_var="http://localhost:5000")
MLFLOW_EXPERIMENT_NAME = Variable.get("mlflow_experiment_name", default_var="sentiment-analysis-pipeline")
DATA_INPUT_PATH = Variable.get("data_input_path", default_var="/home/felipe/sentiment-project/data_raw/sentiment140/training.1600000.processed.noemoticon.csv")
DATA_OUTPUT_PATH = Variable.get("data_output_path", default_var="/home/felipe/sentiment-project/data_clean")
MODEL_OUTPUT_PATH = Variable.get("model_output_path", default_var="/home/felipe/sentiment-project/models")
CODE_PATH = Variable.get("code_path", default_var="/home/felipe/sentiment-project/code")

# GCP Dataproc Configuration
TEST_MODE = Variable.get("test_mode", default_var="true").lower() == "true"  # Set to false for production
GCP_PROJECT = Variable.get("gcp_project", default_var="sentiment-analysis-140")
GCP_REGION = Variable.get("gcp_region", default_var="us-central1")
DATAPROC_CLUSTER = Variable.get("dataproc_cluster", default_var="sentiment-cluster")
GCS_BUCKET = Variable.get("gcs_bucket", default_var="sentiment-bucket")
MLFLOW_VM_IP = Variable.get("mlflow_vm_ip", default_var="34.66.73.187")
MLFLOW_PORT = Variable.get("mlflow_port", default_var="5000")
GCP_CONN_ID = Variable.get("gcp_conn_id", default_var="google_cloud_default")

# GCS Paths
GCS_CODE_PATH = f"gs://{GCS_BUCKET}-code"
GCS_DATA_PATH = f"gs://{GCS_BUCKET}-raw"
GCS_OUTPUT_PATH = f"gs://{GCS_BUCKET}-artifacts"
GCS_JARS_PATH = f"gs://{GCS_BUCKET}-jars"

# Default arguments for the DAG
default_args = {
    'owner': 'data-science-team',
    'depends_on_past': False,
    'start_date': datetime(2025, 12, 20),
    'email': ['datascience@example.com'],
    'email_on_failure': True,
    'email_on_retry': False,
    'retries': 2,
    'retry_delay': timedelta(minutes=5),
    'execution_timeout': timedelta(hours=4),
}

# Define the DAG
dag = DAG(
    'sentiment_ml_training_pipeline',
    default_args=default_args,
    description='End-to-end ML training pipeline for sentiment analysis with MLflow',
    schedule_interval='@weekly',  # Run weekly, adjust as needed
    catchup=False,
    max_active_runs=1,
    tags=['ml', 'sentiment-analysis', 'spark', 'mlflow'],
)


def validate_data(**context):
    """Validate that input data exists and has the correct format"""
    import os
    from pyspark.sql import SparkSession
    
    logging.info(f"Validating data at: {DATA_INPUT_PATH}")
    
    if not os.path.exists(DATA_INPUT_PATH):
        raise FileNotFoundError(f"Input data not found at: {DATA_INPUT_PATH}")
    
    # Create Spark session for validation
    spark = SparkSession.builder.appName("data-validation").getOrCreate()
    
    try:
        # Read first few rows to validate format
        df = spark.read.csv(DATA_INPUT_PATH, header=False, inferSchema=True)
        row_count = df.count()
        
        if row_count == 0:
            raise ValueError("Input data is empty")
        
        logging.info(f"Data validation successful. Total rows: {row_count}")
        
        # Push metadata to XCom
        context['task_instance'].xcom_push(key='data_row_count', value=row_count)
        
        return row_count
        
    finally:
        spark.stop()


def check_mlflow_connection(**context):
    """Check if MLflow tracking server is accessible"""
    import mlflow
    import requests
    
    logging.info(f"Checking MLflow connection at: {MLFLOW_TRACKING_URI}")
    
    try:
        mlflow.set_tracking_uri(MLFLOW_TRACKING_URI)
        
        # Try to get or create experiment
        experiment = mlflow.get_experiment_by_name(MLFLOW_EXPERIMENT_NAME)
        if experiment is None:
            experiment_id = mlflow.create_experiment(MLFLOW_EXPERIMENT_NAME)
            logging.info(f"Created new MLflow experiment: {MLFLOW_EXPERIMENT_NAME} (ID: {experiment_id})")
        else:
            logging.info(f"MLflow experiment exists: {MLFLOW_EXPERIMENT_NAME} (ID: {experiment.experiment_id})")
        
        context['task_instance'].xcom_push(key='mlflow_connected', value=True)
        
    except Exception as e:
        logging.error(f"Failed to connect to MLflow: {str(e)}")
        raise


def prepare_training_environment(**context):
    """Prepare directories and environment for training"""
    import os
    
    directories = [
        DATA_OUTPUT_PATH,
        MODEL_OUTPUT_PATH,
        os.path.join(MODEL_OUTPUT_PATH, "checkpoints"),
        os.path.join(DATA_OUTPUT_PATH, "processed"),
    ]
    
    for directory in directories:
        os.makedirs(directory, exist_ok=True)
        logging.info(f"Ensured directory exists: {directory}")
    
    # Push metadata
    context['task_instance'].xcom_push(key='environment_ready', value=True)


def get_best_model_info(**context):
    """Retrieve best model information from MLflow"""
    import mlflow
    from mlflow.tracking import MlflowClient
    
    mlflow.set_tracking_uri(MLFLOW_TRACKING_URI)
    client = MlflowClient()
    
    experiment = mlflow.get_experiment_by_name(MLFLOW_EXPERIMENT_NAME)
    if not experiment:
        logging.warning("No experiment found yet")
        return None
    
    # Get all runs from the experiment, sorted by accuracy
    runs = mlflow.search_runs(
        experiment_ids=[experiment.experiment_id],
        order_by=["metrics.accuracy DESC"],
        max_results=1
    )
    
    if len(runs) == 0:
        logging.warning("No runs found in experiment")
        return None
    
    best_run = runs.iloc[0]
    
    best_model_info = {
        'run_id': best_run.run_id,
        'accuracy': best_run['metrics.accuracy'],
        'auc': best_run['metrics.auc'],
        'model_type': best_run['params.model_type'] if 'params.model_type' in best_run else 'unknown',
        'run_name': best_run['tags.mlflow.runName'] if 'tags.mlflow.runName' in best_run else 'unknown'
    }
    
    logging.info(f"Best model: {best_model_info['model_type']} "
                f"(Accuracy: {best_model_info['accuracy']:.4f}, AUC: {best_model_info['auc']:.4f})")
    
    context['task_instance'].xcom_push(key='best_model_info', value=best_model_info)
    
    return best_model_info


def send_completion_notification(**context):
    """Send notification with training results"""
    best_model_info = context['task_instance'].xcom_pull(
        task_ids='get_best_model_info', 
        key='best_model_info'
    )
    
    row_count = context['task_instance'].xcom_pull(
        task_ids='validate_data', 
        key='data_row_count'
    )
    
    if best_model_info:
        message = f"""
    Sentiment Analysis ML Training Pipeline Completed Successfully!
    
    Dataset: {row_count:,} rows
    MLflow Experiment: {MLFLOW_EXPERIMENT_NAME}
    MLflow URI: {MLFLOW_TRACKING_URI}
    
    Best Model Results:
    - Model Type: {best_model_info.get('model_type', 'N/A')}
    - Accuracy: {best_model_info.get('accuracy', 0):.4f}
    - AUC: {best_model_info.get('auc', 0):.4f}
    - Run ID: {best_model_info.get('run_id', 'N/A')}
    
    View results in MLflow: {MLFLOW_TRACKING_URI}
    """
    else:
        message = f"""
    Sentiment Analysis ML Training Pipeline Completed!
    
    Mode: TEST MODE (no models trained)
    Dataset: {row_count:,} rows validated
    
    To run actual training on Dataproc:
    1. Set: airflow variables set test_mode false
    2. Configure GCP connection: airflow connections add google_cloud_default --conn-type google_cloud_platform
    3. Trigger the DAG again
    """
    
    logging.info(message)
    
    # Here you can add email/Slack notification logic
    # Example: send_slack_notification(message)
    # Example: send_email(to='team@example.com', subject='Training Complete', body=message)


# Task: Start
start = DummyOperator(
    task_id='start',
    dag=dag,
)

# Task: Validate input data
validate_data_task = PythonOperator(
    task_id='validate_data',
    python_callable=validate_data,
    dag=dag,
)

# Task: Check MLflow connection
check_mlflow_task = PythonOperator(
    task_id='check_mlflow_connection',
    python_callable=check_mlflow_connection,
    dag=dag,
)

# Task: Prepare environment
prepare_env_task = PythonOperator(
    task_id='prepare_environment',
    python_callable=prepare_training_environment,
    dag=dag,
)

# Task Group: Model Training with Spark on Dataproc
with TaskGroup('model_training', tooltip='Train multiple ML models on Dataproc', dag=dag) as model_training_group:
    
    if TEST_MODE:
        # Task: Simulate training job for testing
        train_models_full = BashOperator(
            task_id='train_models_full_dataset',
            bash_command=f"""
            echo "TEST MODE: Would submit Dataproc job with:"
            echo "Cluster: {DATAPROC_CLUSTER}"
            echo "Region: {GCP_REGION}"
            echo "Project: {GCP_PROJECT}"
            echo "Code: {GCS_CODE_PATH}/sentiment_multimodel.py"
            echo "Input: {GCS_DATA_PATH}/training.1600000.processed.noemoticon.csv"
            echo "Output: {GCS_OUTPUT_PATH}"
            echo "MLflow: http://{MLFLOW_VM_IP}:{MLFLOW_PORT}"
            echo ""
            echo "To run in production, set Airflow variable: airflow variables set test_mode false"
            """,
            dag=dag,
        )
    else:
        # Task: Train models with full dataset on Dataproc
        train_models_full = DataprocSubmitPySparkJobOperator(
            task_id='train_models_full_dataset',
            main=f'{GCS_CODE_PATH}/sentiment_multimodel.py',
            cluster_name=DATAPROC_CLUSTER,
            region=GCP_REGION,
            project_id=GCP_PROJECT,
            gcp_conn_id=GCP_CONN_ID,
            arguments=[
                '--input', f'{GCS_DATA_PATH}/training.1600000.processed.noemoticon.csv',
                '--output', GCS_OUTPUT_PATH,
                '--mlflow-uri', f'http://{MLFLOW_VM_IP}:{MLFLOW_PORT}',
                '--mlflow-experiment', MLFLOW_EXPERIMENT_NAME,
                '--registered-model-name', 'twitter-sentiment-model',
                '--num-features', '32768',
                '--train-ratio', '0.8',
                '--val-ratio', '0.1',
                '--enable-caching',
                '--sample-fraction', '1.0',
            ],
            dataproc_jars=[f'{GCS_JARS_PATH}/spark-nlp_2.12-4.4.0.jar'],
            dataproc_properties={
                'spark.driver.memory': '8g',
                'spark.executor.memory': '16g',
                'spark.executor.cores': '4',
                'spark.sql.shuffle.partitions': '200',
            },
            dag=dag,
        )
    
    if TEST_MODE:
        # Task: Simulate CV training job for testing
        train_models_cv = BashOperator(
            task_id='train_models_with_cv',
            bash_command=f"""
            echo "TEST MODE: Would submit Dataproc CV job with:"
            echo "Cluster: {DATAPROC_CLUSTER}"
            echo "Region: {GCP_REGION}"
            echo "Sample: 30%"
            echo "CV Folds: 2"
            echo "Features: 16384"
            """,
            dag=dag,
        )
    else:
        # Task: Quick training with hyperparameter tuning (smaller sample)
        train_models_cv = DataprocSubmitPySparkJobOperator(
            task_id='train_models_with_cv',
            main=f'{GCS_CODE_PATH}/sentiment_multimodel.py',
            cluster_name=DATAPROC_CLUSTER,
            region=GCP_REGION,
            project_id=GCP_PROJECT,
            gcp_conn_id=GCP_CONN_ID,
            arguments=[
                '--input', f'{GCS_DATA_PATH}/training.1600000.processed.noemoticon.csv',
                '--output', GCS_OUTPUT_PATH,
                '--mlflow-uri', f'http://{MLFLOW_VM_IP}:{MLFLOW_PORT}',
                '--mlflow-experiment', f'{MLFLOW_EXPERIMENT_NAME}-cv',
                '--registered-model-name', 'twitter-sentiment-model-cv',
                '--num-features', '16384',
                '--train-ratio', '0.8',
                '--val-ratio', '0.1',
                '--enable-cv',
                '--cv-folds', '2',
                '--enable-caching',
                '--sample-fraction', '0.3',  # Use 30% for faster CV
            ],
            dataproc_jars=[f'{GCS_JARS_PATH}/spark-nlp_2.12-4.4.0.jar'],
            dataproc_properties={
                'spark.driver.memory': '8g',
                'spark.executor.memory': '16g',
                'spark.executor.cores': '4',
                'spark.sql.shuffle.partitions': '100',
            },
            dag=dag,
        )
    
    # Both training tasks can run in parallel
    [train_models_full, train_models_cv]

# Task: Retrieve best model information
get_best_model_task = PythonOperator(
    task_id='get_best_model_info',
    python_callable=get_best_model_info,
    dag=dag,
)

# Task: Generate training report
generate_report_task = BashOperator(
    task_id='generate_training_report',
    bash_command=f"""
    echo "=== Sentiment Analysis Training Report ===" > {MODEL_OUTPUT_PATH}/training_report.txt
    echo "Date: $(date)" >> {MODEL_OUTPUT_PATH}/training_report.txt
    echo "MLflow Experiment: {MLFLOW_EXPERIMENT_NAME}" >> {MODEL_OUTPUT_PATH}/training_report.txt
    echo "Data Path: {DATA_INPUT_PATH}" >> {MODEL_OUTPUT_PATH}/training_report.txt
    echo "" >> {MODEL_OUTPUT_PATH}/training_report.txt
    ls -lh {MODEL_OUTPUT_PATH}/models/ >> {MODEL_OUTPUT_PATH}/training_report.txt 2>/dev/null || echo "No models found"
    """,
    dag=dag,
)

# Task: Send completion notification
notify_task = PythonOperator(
    task_id='send_completion_notification',
    python_callable=send_completion_notification,
    dag=dag,
)

# Task: End
end = DummyOperator(
    task_id='end',
    dag=dag,
)

# Define task dependencies
start >> [validate_data_task, check_mlflow_task] >> prepare_env_task
prepare_env_task >> model_training_group
model_training_group >> get_best_model_task >> generate_report_task
generate_report_task >> notify_task >> end
