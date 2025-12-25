# Airflow with Spark and MLflow - Sentiment Analysis Pipeline

This directory contains an Apache Airflow DAG for orchestrating the sentiment analysis ML training pipeline with MLflow experiment tracking.

## Architecture

```
┌─────────────────────────────────────────────────────────────┐
│                    Airflow Scheduler                         │
│  ┌───────────────────────────────────────────────────────┐  │
│  │  1. Data Validation                                    │  │
│  │  2. MLflow Connection Check                            │  │
│  │  3. Environment Preparation                            │  │
│  │  4. Model Training (Spark)                             │  │
│  │     ├── Full Dataset Training                          │  │
│  │     └── Cross-Validation Training                      │  │
│  │  5. Best Model Selection                               │  │
│  │  6. Report Generation                                  │  │
│  │  7. Notifications                                      │  │
│  └───────────────────────────────────────────────────────┘  │
└─────────────────────────────────────────────────────────────┘
         │                              │
         ▼                              ▼
   ┌──────────┐                  ┌──────────┐
   │  Spark   │                  │  MLflow  │
   │  (Local) │                  │  Server  │
   └──────────┘                  └──────────┘
```

## Components

### 1. **DAG: `sentiment_ml_training_pipeline`**
   - **Schedule**: Weekly (configurable)
   - **Purpose**: End-to-end ML training pipeline
   - **Tasks**:
     - Data validation
     - MLflow connection check
     - Environment preparation
     - Model training (multiple algorithms)
     - Best model selection
     - Report generation
     - Notifications

### 2. **Services** (via Docker Compose)
   - **Airflow Webserver**: UI for DAG management
   - **Airflow Scheduler**: Task scheduling and orchestration
   - **Airflow Worker**: Task execution (Celery)
   - **PostgreSQL**: Airflow metadata database
   - **Redis**: Celery message broker
   - **MLflow**: Experiment tracking server

## Setup Instructions

### Prerequisites
- Docker and Docker Compose installed
- At least 8GB RAM available
- Input data at: `../data_raw/sentiment140/training.1600000.processed.noemoticon.csv`

### Quick Start

1. **Navigate to airflow directory**:
   ```bash
   cd /home/felipe/sentiment-project/airflow
   ```

2. **Run setup script**:
   ```bash
   chmod +x setup.sh
   ./setup.sh
   ```

3. **Access UIs**:
   - Airflow: http://localhost:8080 (airflow/airflow)
   - MLflow: http://localhost:5000

### Manual Setup

1. **Set environment variables**:
   ```bash
   export AIRFLOW_UID=$(id -u)
   echo "AIRFLOW_UID=$AIRFLOW_UID" > .env
   ```

2. **Create directories**:
   ```bash
   mkdir -p dags logs plugins config
   ```

3. **Initialize Airflow**:
   ```bash
   docker-compose up airflow-init
   ```

4. **Start services**:
   ```bash
   docker-compose up -d
   ```

5. **Create Spark connection for GCP Dataproc**:
   ```bash
   # For GCP Dataproc cluster
   docker-compose exec airflow-webserver airflow connections add 'spark_default' \
       --conn-type 'spark' \
       --conn-host 'yarn' \
       --conn-extra '{
         "deploy-mode": "cluster",
         "spark-home": "/usr/lib/spark",
         "project-id": "YOUR_GCP_PROJECT_ID",
         "region": "YOUR_DATAPROC_REGION",
         "cluster-name": "YOUR_DATAPROC_CLUSTER_NAME"
       }'
   
   # Or for direct master node connection
   docker-compose exec airflow-webserver airflow connections add 'spark_default' \
       --conn-type 'spark' \
       --conn-host 'DATAPROC_MASTER_IP' \
       --conn-port '8998' \
       --conn-extra '{"deploy-mode": "cluster"}'
   ```

6. **Set Airflow variables**:
   ```bash
   docker-compose exec airflow-webserver airflow variables set mlflow_tracking_uri "http://mlflow:5000"
   docker-compose exec airflow-webserver airflow variables set mlflow_experiment_name "sentiment-analysis-pipeline"
   ```

## Configuration

### Airflow Variables

Configure these in the Airflow UI (Admin → Variables) or via CLI:

| Variable | Default | Description |
|----------|---------|-------------|
| `mlflow_tracking_uri` | `http://mlflow:5000` | MLflow tracking server URL |
| `mlflow_experiment_name` | `sentiment-analysis-pipeline` | MLflow experiment name |
| `data_input_path` | `/opt/airflow/data/raw/...` | Input CSV file path |
| `data_output_path` | `/opt/airflow/data/clean` | Processed data output |
| `model_output_path` | `/opt/airflow/models` | Model artifacts output |
| `code_path` | `/opt/airflow/code` | Training scripts location |

### DAG Parameters

Modify in `dags/sentiment_training_pipeline.py`:

```python
# Schedule
schedule_interval='@weekly'  # or '@daily', '@monthly', etc.

# Training configuration
'--sample-fraction', '1.0'  # Use 100% of data
'--enable-cv'                # Enable cross-validation
'--cv-folds', '2'            # Number of CV folds
```

## Usage

### Trigger DAG Manually

1. Go to Airflow UI: http://localhost:8080
2. Find DAG: `sentiment_ml_training_pipeline`
3. Toggle to enable (if paused)
4. Click "Trigger DAG" button

### Via CLI

```bash
docker-compose exec airflow-scheduler airflow dags trigger sentiment_ml_training_pipeline
```

### Monitor Execution

```bash
# View scheduler logs
docker-compose logs -f airflow-scheduler

# View worker logs
docker-compose logs -f airflow-worker

# View MLflow logs
docker-compose logs -f mlflow
```

## Pipeline Details

### Task Flow

```
start
  │
  ├─→ validate_data ────────┐
  │                         │
  └─→ check_mlflow_connection ┘
              │
              ▼
     prepare_environment
              │
              ▼
      model_training_group
       ├─→ train_models_full_dataset (100% data, 4 models)
       └─→ train_models_with_cv (30% data, CV tuning)
              │
              ▼
      get_best_model_info
              │
              ▼
   generate_training_report
              │
              ▼
  send_completion_notification
              │
              ▼
            end
```

### Models Trained

1. **Logistic Regression** - Fast, interpretable baseline
2. **Random Forest** - Ensemble method with good accuracy
3. **Gradient Boosted Trees (GBT)** - High-performance ensemble
4. **Linear SVC** - Support vector classifier

Each model is:
- Trained on 80% of data
- Validated on 10% of data
- Tested on 10% of data
- Logged to MLflow with metrics and parameters
- Registered in MLflow Model Registry

### Metrics Tracked

- **Accuracy** (primary metric for balanced dataset)
- **AUC** (Area Under ROC Curve)
- **Precision, Recall, F1** (per class and weighted)
- **Confusion Matrix** (as artifact)
- **Training Time**

## MLflow Integration

### View Experiments

1. Open MLflow UI: http://localhost:5000
2. Navigate to experiment: `sentiment-analysis-pipeline`
3. Compare runs by metrics
4. View confusion matrix plots
5. Download registered models

### Access Programmatically

```python
import mlflow

mlflow.set_tracking_uri("http://localhost:5000")

# Get best run
experiment = mlflow.get_experiment_by_name("sentiment-analysis-pipeline")
runs = mlflow.search_runs(
    experiment_ids=[experiment.experiment_id],
    order_by=["metrics.accuracy DESC"],
    max_results=1
)
best_run = runs.iloc[0]
print(f"Best accuracy: {best_run['metrics.accuracy']}")
```

## Troubleshooting

### Service Not Starting

```bash
# Check logs
docker-compose logs airflow-webserver
docker-compose logs airflow-scheduler

# Restart services
docker-compose restart

# Full reset
docker-compose down -v
./setup.sh
```

### Spark Submit Fails

Check Spark connection:
```bash
docker-compose exec airflow-webserver airflow connections list | grep spark
```

For GCP Dataproc:
- Verify cluster is running: `gcloud dataproc clusters list`
- Check firewall allows connection from Airflow
- Ensure service account has Dataproc permissions
- Test connection: `gcloud dataproc jobs submit spark --cluster=CLUSTER_NAME --region=REGION --class=org.apache.spark.examples.SparkPi --jars=file:///usr/lib/spark/examples/jars/spark-examples.jar -- 10`

### MLflow Connection Error

Verify MLflow is running:
```bash
docker-compose ps mlflow
curl http://localhost:5000/health
```

### Out of Memory

Reduce sample size in DAG:
```python
'--sample-fraction', '0.5'  # Use 50% of data
```

Or increase Docker resources:
- Docker Desktop → Settings → Resources
- Minimum: 8GB RAM, 4 CPUs

## Monitoring & Alerts

### Email Notifications

Configure in `docker-compose.yml`:
```yaml
AIRFLOW__SMTP__SMTP_HOST: smtp.gmail.com
AIRFLOW__SMTP__SMTP_USER: your-email@gmail.com
AIRFLOW__SMTP__SMTP_PASSWORD: your-app-password
AIRFLOW__SMTP__SMTP_PORT: 587
```

### Slack Notifications

Install Slack provider:
```bash
docker-compose exec airflow-webserver pip install apache-airflow-providers-slack
```

Add Slack webhook in DAG:
```python
from airflow.providers.slack.operators.slack_webhook import SlackWebhookOperator

notify_slack = SlackWebhookOperator(
    task_id='slack_notification',
    slack_webhook_conn_id='slack_webhook',
    message='Training complete! Best accuracy: {{ ti.xcom_pull(...) }}',
)
```

## Production Considerations

### Scaling

1. **Celery Workers**: Increase worker replicas
   ```bash
   docker-compose up -d --scale airflow-worker=3
   ```

2. **Spark Cluster**: Use external Spark cluster
   - Update Spark connection to point to cluster master
   - Adjust `--deploy-mode` to `cluster`

3. **MLflow**: Use external database and S3 for artifacts
   ```bash
   mlflow server \
     --backend-store-uri postgresql://user:pass@host/db \
     --default-artifact-root s3://bucket/path
   ```

### Security

1. **Change default passwords** in `.env`
2. **Use secrets management**: AWS Secrets Manager, HashiCorp Vault
3. **Enable RBAC** in Airflow
4. **Use HTTPS** for production deployments

### Monitoring

1. **Prometheus + Grafana**: Monitor Airflow metrics
2. **Flower**: Monitor Celery workers (http://localhost:5555)
3. **CloudWatch/Datadog**: Infrastructure monitoring

## Cost Optimization

1. **Use smaller samples for CV**: `--sample-fraction 0.3`
2. **Reduce model complexity**: Fewer trees, lower depth
3. **Schedule during off-peak**: `schedule_interval='0 2 * * 0'`  # 2 AM Sunday
4. **Use spot instances**: For cloud deployments

## Maintenance

### Clean Up Old Data

```bash
# Remove old logs (older than 30 days)
docker-compose exec airflow-scheduler airflow db clean --clean-before-timestamp $(date -d '30 days ago' +%Y-%m-%d)

# Prune MLflow experiments
docker-compose exec mlflow python -c "
import mlflow
mlflow.set_tracking_uri('http://localhost:5000')
# Delete old runs programmatically
"
```

### Backup

```bash
# Backup Airflow database
docker-compose exec postgres pg_dump -U airflow airflow > airflow_backup.sql

# Backup MLflow artifacts
docker cp $(docker-compose ps -q mlflow):/mlflow/artifacts ./mlflow_backup
```

## Resources

- [Apache Airflow Documentation](https://airflow.apache.org/docs/)
- [MLflow Documentation](https://mlflow.org/docs/latest/index.html)
- [PySpark Documentation](https://spark.apache.org/docs/latest/api/python/)

## Support

For issues or questions:
1. Check logs: `docker-compose logs -f`
2. Review Airflow UI task logs
3. Check MLflow UI for experiment details
