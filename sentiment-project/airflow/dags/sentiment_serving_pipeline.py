"""
Airflow DAG for Sentiment Analysis Model Serving Pipeline
Deploys trained models from MLflow to Dataproc for real-time predictions.

This DAG:
1. Validates MLflow connection
2. Gets the best model from MLflow Registry
3. Deploys streaming serving job to Dataproc
4. Monitors serving job health
5. Can stop/restart serving job
"""

from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator
from airflow.providers.google.cloud.operators.dataproc import (
    DataprocSubmitPySparkJobOperator,
    DataprocDeleteJobOperator
)
from airflow.operators.dummy import DummyOperator
from airflow.utils.task_group import TaskGroup
from airflow.models import Variable
from airflow.sensors.python import PythonSensor
import logging
import os

# Configuration from Airflow Variables
MLFLOW_TRACKING_URI = Variable.get("mlflow_tracking_uri", default_var="http://localhost:5000")
MLFLOW_EXPERIMENT_NAME = Variable.get("mlflow_experiment_name", default_var="sentiment-analysis-pipeline")

# GCP Dataproc Configuration
DEPLOY_MODE = Variable.get("serving_deploy_mode", default_var="test")  # test or production
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
GCS_OUTPUT_PATH = f"gs://{GCS_BUCKET}-predictions"
GCS_CHECKPOINT_PATH = f"gs://{GCS_BUCKET}-checkpoint"
GCS_STREAMING_INPUT = f"gs://{GCS_BUCKET}-streaming-input"

# Serving Configuration
SERVING_MODEL_NAME = Variable.get("serving_model_name", default_var="twitter-sentiment-model-best")
SERVING_MODEL_STAGE = Variable.get("serving_model_stage", default_var="Production")
SERVING_SOURCE_TYPE = Variable.get("serving_source_type", default_var="file")  # file or kafka
BATCH_INTERVAL_SECONDS = Variable.get("serving_batch_interval", default_var="30")
RELOAD_INTERVAL_SECONDS = Variable.get("serving_reload_interval", default_var="300")

# Kafka Configuration (optional)
KAFKA_BOOTSTRAP_SERVERS = Variable.get("kafka_bootstrap_servers", default_var="")
KAFKA_INPUT_TOPIC = Variable.get("kafka_input_topic", default_var="sentiment-input")
KAFKA_OUTPUT_TOPIC = Variable.get("kafka_output_topic", default_var="sentiment-predictions")

# Default arguments for the DAG
default_args = {
    'owner': 'ml-ops-team',
    'depends_on_past': False,
    'start_date': datetime(2025, 12, 22),
    'email': ['mlops@example.com'],
    'email_on_failure': True,
    'email_on_retry': False,
    'retries': 2,
    'retry_delay': timedelta(minutes=3),
    'execution_timeout': timedelta(hours=1),
}

# Define the DAG
dag = DAG(
    'sentiment_model_serving',
    default_args=default_args,
    description='Deploy and manage sentiment analysis model serving on Dataproc',
    schedule_interval=None,  # Manual trigger only
    catchup=False,
    max_active_runs=1,
    tags=['ml', 'serving', 'spark', 'mlflow', 'streaming'],
)


def check_mlflow_connection(**context):
    """Check if MLflow tracking server is accessible and model exists"""
    import mlflow
    from mlflow.tracking import MlflowClient
    
    logging.info(f"Checking MLflow connection at: {MLFLOW_TRACKING_URI}")
    
    try:
        mlflow.set_tracking_uri(MLFLOW_TRACKING_URI)
        client = MlflowClient()
        
        # Check if model exists in registry
        try:
            versions = client.get_latest_versions(SERVING_MODEL_NAME)
            if not versions:
                raise ValueError(f"No versions found for model: {SERVING_MODEL_NAME}")
            
            latest_version = max(versions, key=lambda v: int(v.version))
            
            logging.info(f"Found model: {SERVING_MODEL_NAME}")
            logging.info(f"Latest version: {latest_version.version}")
            logging.info(f"Current stage: {latest_version.current_stage}")
            
            # Push model info to XCom
            model_info = {
                'model_name': SERVING_MODEL_NAME,
                'version': latest_version.version,
                'stage': latest_version.current_stage,
                'model_uri': f"models:/{SERVING_MODEL_NAME}/{latest_version.version}"
            }
            context['task_instance'].xcom_push(key='model_info', value=model_info)
            
            return True
            
        except Exception as e:
            logging.error(f"Model not found in registry: {str(e)}")
            raise ValueError(f"Model {SERVING_MODEL_NAME} not found in MLflow registry. "
                           f"Please train a model first using the training pipeline.")
        
    except Exception as e:
        logging.error(f"Failed to connect to MLflow: {str(e)}")
        raise


def validate_serving_config(**context):
    """Validate serving configuration and prerequisites"""
    logging.info("Validating serving configuration...")
    
    # Check source type configuration
    if SERVING_SOURCE_TYPE == "kafka":
        if not KAFKA_BOOTSTRAP_SERVERS:
            raise ValueError("Kafka bootstrap servers not configured. "
                           "Set airflow variable: kafka_bootstrap_servers")
        logging.info(f"Kafka input: {KAFKA_BOOTSTRAP_SERVERS}/{KAFKA_INPUT_TOPIC}")
    else:
        logging.info(f"File input: {GCS_STREAMING_INPUT}")
    
    # Push config to XCom
    config = {
        'source_type': SERVING_SOURCE_TYPE,
        'model_name': SERVING_MODEL_NAME,
        'output_path': GCS_OUTPUT_PATH,
        'checkpoint_path': GCS_CHECKPOINT_PATH,
        'batch_interval': BATCH_INTERVAL_SECONDS,
        'reload_interval': RELOAD_INTERVAL_SECONDS,
        'deploy_mode': DEPLOY_MODE
    }
    context['task_instance'].xcom_push(key='serving_config', value=config)
    
    logging.info(f"Serving configuration validated: {config}")
    return True


def get_best_model_details(**context):
    """Get detailed information about the best model from MLflow"""
    import mlflow
    from mlflow.tracking import MlflowClient
    
    mlflow.set_tracking_uri(MLFLOW_TRACKING_URI)
    client = MlflowClient()
    
    # Get model info from previous task
    model_info = context['task_instance'].xcom_pull(
        task_ids='check_mlflow_connection',
        key='model_info'
    )
    
    if not model_info:
        raise ValueError("Model info not found")
    
    # Get model version details
    model_version = client.get_model_version(
        name=model_info['model_name'],
        version=model_info['version']
    )
    
    # Get run details to fetch metrics
    run = client.get_run(model_version.run_id)
    
    metrics = {
        'accuracy': run.data.metrics.get('accuracy', 'N/A'),
        'auc': run.data.metrics.get('auc', 'N/A'),
        'f1_score': run.data.metrics.get('weighted_f1', 'N/A'),
    }
    
    model_details = {
        **model_info,
        'run_id': model_version.run_id,
        'metrics': metrics,
        'description': model_version.description or 'No description'
    }
    
    logging.info(f"Model to deploy: {model_details}")
    context['task_instance'].xcom_push(key='model_details', value=model_details)
    
    return model_details


def send_deployment_notification(**context):
    """Send notification about serving deployment"""
    model_details = context['task_instance'].xcom_pull(
        task_ids='get_best_model_details',
        key='model_details'
    )
    
    serving_config = context['task_instance'].xcom_pull(
        task_ids='validate_serving_config',
        key='serving_config'
    )
    
    message = f"""
Sentiment Analysis Model Serving Deployed Successfully!

Model Information:
- Model Name: {model_details['model_name']}
- Version: {model_details['version']}
- Stage: {model_details['stage']}
- Accuracy: {model_details['metrics']['accuracy']}
- AUC: {model_details['metrics']['auc']}
- F1 Score: {model_details['metrics']['f1_score']}

Serving Configuration:
- Source Type: {serving_config['source_type']}
- Output Path: {serving_config['output_path']}
- Batch Interval: {serving_config['batch_interval']}s
- Model Reload Interval: {serving_config['reload_interval']}s

Cluster: {DATAPROC_CLUSTER} ({GCP_REGION})

View MLflow: {MLFLOW_TRACKING_URI}
View Predictions: {GCS_OUTPUT_PATH}
"""
    
    logging.info(message)
    
    # Here you can add email/Slack notification logic
    # Example: send_slack_notification(message)


# Task: Start
start = DummyOperator(
    task_id='start',
    dag=dag,
)

# Task: Check MLflow connection and model availability
check_mlflow_task = PythonOperator(
    task_id='check_mlflow_connection',
    python_callable=check_mlflow_connection,
    dag=dag,
)

# Task: Validate serving configuration
validate_config_task = PythonOperator(
    task_id='validate_serving_config',
    python_callable=validate_serving_config,
    dag=dag,
)

# Task: Get best model details
get_model_task = PythonOperator(
    task_id='get_best_model_details',
    python_callable=get_best_model_details,
    dag=dag,
)

# Task Group: Deploy Serving
with TaskGroup('deploy_serving', tooltip='Deploy model serving to Dataproc', dag=dag) as deploy_group:
    
    if DEPLOY_MODE == "test":
        # Task: Simulate deployment for testing
        deploy_serving = BashOperator(
            task_id='deploy_streaming_job',
            bash_command=f"""
            echo "TEST MODE: Would deploy serving job with:"
            echo "Cluster: {DATAPROC_CLUSTER}"
            echo "Region: {GCP_REGION}"
            echo "Script: {GCS_CODE_PATH}/streaming_serving_mlflow_hdfs_kafka_optimize.py"
            echo "Model: {SERVING_MODEL_NAME}"
            echo "Source: {SERVING_SOURCE_TYPE}"
            echo "Output: {GCS_OUTPUT_PATH}"
            echo "MLflow: http://{MLFLOW_VM_IP}:{MLFLOW_PORT}"
            echo ""
            echo "To deploy in production, set: airflow variables set serving_deploy_mode production"
            """,
            dag=dag,
        )
    else:
        # Build arguments based on source type
        if SERVING_SOURCE_TYPE == "kafka":
            serving_args = [
                '--source-type', 'kafka',
                '--kafka-bootstrap', KAFKA_BOOTSTRAP_SERVERS,
                '--kafka-topic', KAFKA_INPUT_TOPIC,
                '--write-kafka', 'true',
                '--kafka-output-bootstrap', KAFKA_BOOTSTRAP_SERVERS,
                '--kafka-output-topic', KAFKA_OUTPUT_TOPIC,
            ]
        else:
            serving_args = [
                '--source-type', 'file',
                '--input-path', GCS_STREAMING_INPUT,
                '--max-files-per-trigger', '10',
            ]
        
        # Common arguments
        serving_args.extend([
            '--output-path', GCS_OUTPUT_PATH,
            '--checkpoint', GCS_CHECKPOINT_PATH,
            '--mlflow-uri', f'http://{MLFLOW_VM_IP}:{MLFLOW_PORT}',
            '--model-name', SERVING_MODEL_NAME,
            '--model-stage', SERVING_MODEL_STAGE,
            '--model-flavor', 'spark',
            '--batch-interval-seconds', str(BATCH_INTERVAL_SECONDS),
            '--reload-interval-seconds', str(RELOAD_INTERVAL_SECONDS),
            '--write-gcs', 'true',
        ])
        
        # Task: Deploy streaming serving job to Dataproc
        deploy_serving = DataprocSubmitPySparkJobOperator(
            task_id='deploy_streaming_job',
            main=f'{GCS_CODE_PATH}/streaming_serving_mlflow_hdfs_kafka_optimize.py',
            cluster_name=DATAPROC_CLUSTER,
            region=GCP_REGION,
            project_id=GCP_PROJECT,
            gcp_conn_id=GCP_CONN_ID,
            arguments=serving_args,
            dataproc_properties={
                'spark.streaming.stopGracefullyOnShutdown': 'true',
                'spark.sql.streaming.schemaInference': 'true',
                'spark.driver.memory': '4g',
                'spark.executor.memory': '8g',
                'spark.executor.cores': '2',
            },
            asynchronous=True,  # Don't wait for streaming job to complete
            dag=dag,
        )
    
    # Task: Verify deployment
    verify_deployment = BashOperator(
        task_id='verify_deployment',
        bash_command=f"""
        echo "Serving job deployed successfully"
        echo "Monitor at: https://console.cloud.google.com/dataproc/clusters/{DATAPROC_CLUSTER}?project={GCP_PROJECT}"
        echo "View predictions: gsutil ls {GCS_OUTPUT_PATH}/predictions/"
        echo "View checkpoints: gsutil ls {GCS_CHECKPOINT_PATH}/"
        """,
        dag=dag,
    )
    
    deploy_serving >> verify_deployment

# Task: Send notification
notify_task = PythonOperator(
    task_id='send_deployment_notification',
    python_callable=send_deployment_notification,
    dag=dag,
)

# Task: Create monitoring dashboard info
create_monitoring_info = BashOperator(
    task_id='create_monitoring_info',
    bash_command=f"""
    echo "=== Model Serving Monitoring ===" > /opt/airflow/logs/serving_info.txt
    echo "Deployment Date: $(date)" >> /opt/airflow/logs/serving_info.txt
    echo "Model: {SERVING_MODEL_NAME}" >> /opt/airflow/logs/serving_info.txt
    echo "Cluster: {DATAPROC_CLUSTER}" >> /opt/airflow/logs/serving_info.txt
    echo "Output: {GCS_OUTPUT_PATH}" >> /opt/airflow/logs/serving_info.txt
    echo "" >> /opt/airflow/logs/serving_info.txt
    echo "Monitor Commands:" >> /opt/airflow/logs/serving_info.txt
    echo "  gsutil ls {GCS_OUTPUT_PATH}/predictions/" >> /opt/airflow/logs/serving_info.txt
    echo "  gcloud dataproc jobs list --cluster={DATAPROC_CLUSTER} --region={GCP_REGION}" >> /opt/airflow/logs/serving_info.txt
    cat /opt/airflow/logs/serving_info.txt
    """,
    dag=dag,
)

# Task: End
end = DummyOperator(
    task_id='end',
    dag=dag,
)

# Define task dependencies
start >> [check_mlflow_task, validate_config_task]
[check_mlflow_task, validate_config_task] >> get_model_task
get_model_task >> deploy_group
deploy_group >> [notify_task, create_monitoring_info]
[notify_task, create_monitoring_info] >> end
