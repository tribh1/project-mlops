"""
Airflow DAG for Managing Sentiment Analysis Serving Jobs
Stop, restart, or check status of serving jobs on Dataproc.

This DAG:
1. Lists active serving jobs
2. Provides options to stop specific jobs
3. Monitors job health and performance
"""

from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator, BranchPythonOperator
from airflow.operators.bash import BashOperator
from airflow.operators.dummy import DummyOperator
from airflow.models import Variable
import logging

# Configuration
GCP_PROJECT = Variable.get("gcp_project", default_var="sentiment-analysis-140")
GCP_REGION = Variable.get("gcp_region", default_var="us-central1")
DATAPROC_CLUSTER = Variable.get("dataproc_cluster", default_var="sentiment-cluster")
GCS_OUTPUT_PATH = Variable.get("gcs_bucket", default_var="sentiment-bucket") + "-predictions"
ACTION = Variable.get("serving_action", default_var="status")  # status, stop, restart

default_args = {
    'owner': 'ml-ops-team',
    'depends_on_past': False,
    'start_date': datetime(2025, 12, 22),
    'email': ['mlops@example.com'],
    'email_on_failure': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=2),
}

dag = DAG(
    'sentiment_serving_management',
    default_args=default_args,
    description='Manage serving jobs - check status, stop, or view metrics',
    schedule_interval=None,  # Manual trigger only
    catchup=False,
    tags=['ml', 'serving', 'management', 'monitoring'],
)


def list_serving_jobs(**context):
    """List all serving jobs running on Dataproc"""
    import subprocess
    import json
    
    logging.info(f"Listing serving jobs on cluster: {DATAPROC_CLUSTER}")
    
    cmd = [
        'gcloud', 'dataproc', 'jobs', 'list',
        f'--cluster={DATAPROC_CLUSTER}',
        f'--region={GCP_REGION}',
        f'--project={GCP_PROJECT}',
        '--filter=status.state=RUNNING OR status.state=PENDING',
        '--format=json'
    ]
    
    try:
        result = subprocess.run(cmd, capture_output=True, text=True, check=True)
        jobs = json.loads(result.stdout) if result.stdout else []
        
        serving_jobs = [
            job for job in jobs 
            if 'streaming_serving' in job.get('reference', {}).get('jobId', '')
        ]
        
        logging.info(f"Found {len(serving_jobs)} serving jobs")
        
        for job in serving_jobs:
            job_id = job['reference']['jobId']
            state = job['status']['state']
            logging.info(f"  Job ID: {job_id}, State: {state}")
        
        context['task_instance'].xcom_push(key='serving_jobs', value=serving_jobs)
        return len(serving_jobs) > 0
        
    except subprocess.CalledProcessError as e:
        logging.error(f"Failed to list jobs: {e.stderr}")
        return False


def check_prediction_output(**context):
    """Check if predictions are being written"""
    import subprocess
    
    logging.info(f"Checking prediction output at: {GCS_OUTPUT_PATH}")
    
    cmd = [
        'gsutil', 'ls', '-lh',
        f'gs://{GCS_OUTPUT_PATH}/predictions/'
    ]
    
    try:
        result = subprocess.run(cmd, capture_output=True, text=True, check=True)
        
        lines = result.stdout.strip().split('\n')
        recent_files = [l for l in lines if 'part-' in l][-5:]  # Last 5 files
        
        logging.info(f"Recent prediction files ({len(recent_files)}):")
        for file in recent_files:
            logging.info(f"  {file}")
        
        return len(recent_files) > 0
        
    except subprocess.CalledProcessError as e:
        logging.warning(f"No predictions found or error: {e.stderr}")
        return False


# Tasks
start = DummyOperator(task_id='start', dag=dag)

list_jobs = PythonOperator(
    task_id='list_serving_jobs',
    python_callable=list_serving_jobs,
    dag=dag,
)

check_output = PythonOperator(
    task_id='check_prediction_output',
    python_callable=check_prediction_output,
    dag=dag,
)

# Show job details
show_job_details = BashOperator(
    task_id='show_job_details',
    bash_command=f"""
    echo "=== Serving Job Status ==="
    echo ""
    echo "Cluster: {DATAPROC_CLUSTER}"
    echo "Region: {GCP_REGION}"
    echo ""
    
    gcloud dataproc jobs list \
      --cluster={DATAPROC_CLUSTER} \
      --region={GCP_REGION} \
      --project={GCP_PROJECT} \
      --filter="status.state=RUNNING OR status.state=PENDING" \
      --format="table(reference.jobId, status.state, placement.clusterName)" \
      2>&1 | head -20
    
    echo ""
    echo "=== Recent Predictions ==="
    gsutil ls -lh gs://{GCS_OUTPUT_PATH}/predictions/ | tail -10 || echo "No predictions found"
    
    echo ""
    echo "=== Prediction Count by Date ==="
    gsutil ls gs://{GCS_OUTPUT_PATH}/predictions/date=* | wc -l || echo "0"
    """,
    dag=dag,
)

# Get job metrics
get_job_metrics = BashOperator(
    task_id='get_job_metrics',
    bash_command=f"""
    echo "=== Job Performance Metrics ==="
    
    # Get the most recent streaming job
    JOB_ID=$(gcloud dataproc jobs list \
      --cluster={DATAPROC_CLUSTER} \
      --region={GCP_REGION} \
      --project={GCP_PROJECT} \
      --filter="status.state=RUNNING" \
      --format="value(reference.jobId)" \
      --limit=1)
    
    if [ -n "$JOB_ID" ]; then
        echo "Job ID: $JOB_ID"
        echo ""
        
        gcloud dataproc jobs describe "$JOB_ID" \
          --region={GCP_REGION} \
          --project={GCP_PROJECT} \
          --format="yaml(status, driverOutputResourceUri)" \
          2>&1 | head -30
        
        echo ""
        echo "View full logs:"
        echo "  gcloud dataproc jobs describe $JOB_ID --region={GCP_REGION}"
    else
        echo "No running jobs found"
    fi
    """,
    dag=dag,
)

# Create monitoring summary
create_summary = BashOperator(
    task_id='create_monitoring_summary',
    bash_command=f"""
    SUMMARY_FILE="/opt/airflow/logs/serving_monitoring_$(date +%Y%m%d_%H%M%S).txt"
    
    echo "=== Sentiment Serving Monitoring Summary ===" > $SUMMARY_FILE
    echo "Generated: $(date)" >> $SUMMARY_FILE
    echo "" >> $SUMMARY_FILE
    
    echo "Cluster: {DATAPROC_CLUSTER}" >> $SUMMARY_FILE
    echo "Region: {GCP_REGION}" >> $SUMMARY_FILE
    echo "Output Path: gs://{GCS_OUTPUT_PATH}" >> $SUMMARY_FILE
    echo "" >> $SUMMARY_FILE
    
    echo "Active Jobs:" >> $SUMMARY_FILE
    gcloud dataproc jobs list \
      --cluster={DATAPROC_CLUSTER} \
      --region={GCP_REGION} \
      --filter="status.state=RUNNING" \
      --format="value(reference.jobId)" >> $SUMMARY_FILE 2>&1 || echo "None" >> $SUMMARY_FILE
    
    echo "" >> $SUMMARY_FILE
    echo "Prediction Files (last 24h):" >> $SUMMARY_FILE
    gsutil ls -l gs://{GCS_OUTPUT_PATH}/predictions/ 2>/dev/null | \
      awk '{{if ($1 != "TOTAL:") print}}' | tail -20 >> $SUMMARY_FILE || echo "None" >> $SUMMARY_FILE
    
    echo "" >> $SUMMARY_FILE
    echo "Commands to manage serving:" >> $SUMMARY_FILE
    echo "  Stop job: gcloud dataproc jobs kill JOB_ID --region={GCP_REGION}" >> $SUMMARY_FILE
    echo "  View logs: gcloud dataproc jobs describe JOB_ID --region={GCP_REGION}" >> $SUMMARY_FILE
    echo "  Restart: Trigger 'sentiment_model_serving' DAG" >> $SUMMARY_FILE
    
    cat $SUMMARY_FILE
    echo ""
    echo "Summary saved to: $SUMMARY_FILE"
    """,
    dag=dag,
)

end = DummyOperator(task_id='end', dag=dag)

# Define dependencies
start >> list_jobs >> [check_output, show_job_details]
show_job_details >> get_job_metrics
[check_output, get_job_metrics] >> create_summary >> end
