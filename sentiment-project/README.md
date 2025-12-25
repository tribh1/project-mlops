# Sentiment Project

End-to-end sentiment analysis ML pipeline with Apache Airflow and MLflow integration.

## Features

- **Multi-model Training**: Logistic Regression, Random Forest, GBT, Linear SVC
- **MLflow Tracking**: Experiment tracking, model registry, and artifact storage
- **Airflow Orchestration**: Automated pipeline for data prep, training, and evaluation
- **Spark Processing**: Scalable data processing and model training
- **Cross-validation**: Hyperparameter tuning with configurable CV folds

## Project Structure
```
sentiment-project/
  airflow/                      # Airflow DAGs and configuration
    dags/
      sentiment_training_pipeline.py    # Production DAG (Docker)
      sentiment_training_local.py       # Development DAG (local)
    docker-compose.yml          # Docker services setup
    setup.sh                    # Docker setup script
    setup_local.sh              # Local setup script
    start_services.sh           # Start all services
    stop_services.sh            # Stop all services
  data_raw/                     # Raw input data
    sentiment140/
      training.1600000.processed.noemoticon.csv
  data_clean/                   # Processed data
  models/                       # Trained model artifacts
  code/                         # Training scripts
    sentiment_multimodel.py     # Multi-model training script
    preprocess.py
    ...
  mlruns/                       # MLflow artifacts (local)
  README.md
```

Quick start

- Create virtualenv and activate:
```bash
python3 -m venv env/venv
source env/venv/bin/activate
pip install -r requirements.txt  # if you add one
```
- Preprocess raw data:
```bash
python code/preprocess.py --input data_raw/ --output data_clean/
```
- Create train/dev/test splits:
```bash
python code/create_splits.py --input data_clean/ --output data_clean/splits/
```
- Train model and log metrics:
```bash
python code/train_and_log.py --data data_clean/splits/
```

Notes
- Scripts are lightweight stubs — implement data-specific logic as needed.

Downloading Sentiment140 and migrating to Google Cloud Storage
-----------------------------------------------------------

Prerequisites
- **Kaggle API token**: place `kaggle.json` in `~/.kaggle/` (or set `KAGGLE_CONFIG_DIR`).
- **Google Cloud auth**: set `GOOGLE_APPLICATION_CREDENTIALS` to a service-account JSON with write access to your GCS bucket.
- **Python deps**: install requirements with `pip install -r requirements.txt`.

Using the helper script
- Download and upload in one step (example):

```bash
python code/download_and_upload.py \
  --dataset kazanova/sentiment140 \
  --output-dir data_raw/sentiment140 \
  --gcs-bucket my-gcs-bucket \
  --gcs-prefix data_raw/sentiment140 \
  --unzip
```

- This will download the dataset to `data_raw/sentiment140`, unzip any `.zip` files (when `--unzip` is used),
  and upload all files under that directory to `gs://my-gcs-bucket/data_raw/sentiment140/`.

Alternative: use `kaggle` + `gsutil` directly
- With the Kaggle CLI installed you can download manually:

```bash
# download dataset to local directory
kaggle datasets download -d kazanova/sentiment140 -p data_raw/sentiment140
# unzip if needed
unzip data_raw/sentiment140/sentiment140.zip -d data_raw/sentiment140
# upload to GCS with gsutil
gsutil -m cp -r data_raw/sentiment140 gs://my-gcs-bucket/data_raw/
```

Environment tips
- If you keep multiple Kaggle credentials, set `KAGGLE_CONFIG_DIR` to the folder containing `kaggle.json` for the process:

```bash
export KAGGLE_CONFIG_DIR=/path/to/kaggle/config
```

- For GCP, authenticate with a service account and point the env var:

```bash
export GOOGLE_APPLICATION_CREDENTIALS=/path/to/service-account.json
```

Verification
- List uploaded files:

```bash
gsutil ls gs://my-gcs-bucket/data_raw/sentiment140/**
```

Small quick run checklist
- Create and activate virtualenv, install deps:

```bash
python3 -m venv env/venv
source env/venv/bin/activate
pip install -r requirements.txt
```

- Ensure credentials are available:

```bash
export KAGGLE_CONFIG_DIR=$HOME/.kaggle
export GOOGLE_APPLICATION_CREDENTIALS=/path/to/service-account.json
```

- Run the script (example):

```bash
python code/download_and_upload.py --gcs-bucket my-gcs-bucket --unzip
```

Security
- Keep `kaggle.json` and your GCP service-account JSON out of source control. Add them to `.gitignore`.
 
Example: full flow

```bash
# set up credentials
export KAGGLE_CONFIG_DIR=$HOME/.kaggle
export GOOGLE_APPLICATION_CREDENTIALS=$HOME/.gcp/service-account.json

# create env and install deps
python3 -m venv env/venv
source env/venv/bin/activate
pip install -r requirements.txt

# download and upload (defaults to kazanova/sentiment140)
python code/download_and_upload.py --gcs-bucket my-gcs-bucket --output-dir data_raw/sentiment140 --gcs-prefix data_raw/sentiment140 --unzip

# verify
gsutil ls gs://my-gcs-bucket/data_raw/sentiment140/**
```

Miniconda GCS fallback (optional)
--------------------------------
If your Dataproc nodes cannot reach the Miniconda repo, upload the installer to GCS and pass the URI to the init action via the env var `MINICONDA_GCS_URI`.

```bash
# upload installer once from a machine with good network
gsutil cp /path/to/Miniconda3-latest-Linux-x86_64.sh gs://my-gcs-bucket/installers/Miniconda3-latest-Linux-x86_64.sh

# when creating the cluster, pass the env var to the init action using metadata
# (gcloud example)
gcloud dataproc clusters create "$CLUSTER_NAME" \
  --region "$REGION" \
  --initialization-actions "gs://$PROJECT_ID-dataproc-inits/init-conda-spark-nlp-mlflow.sh" \
  --metadata MINICONDA_GCS_URI=gs://my-gcs-bucket/installers/Miniconda3-latest-Linux-x86_64.sh \
  --image-version=2.2-ubuntu22 ...
```

---

## ML Pipeline: Training and Serving

This section documents the current production ML pipeline for sentiment analysis, designed for CI/CD automation.

### Architecture Overview

```
┌─────────────────────────────────────────────────────────────────┐
│                      ML Pipeline Architecture                    │
├─────────────────────────────────────────────────────────────────┤
│                                                                   │
│  1. TRAINING PIPELINE                                            │
│     ┌──────────┐    ┌──────────────┐    ┌──────────────┐       │
│     │   GCS    │───▶│  Dataproc    │───▶│   MLflow     │       │
│     │  Bucket  │    │ Spark Submit │    │   Server     │       │
│     │ (script) │    │  (Training)  │    │(Best Model)  │       │
│     └──────────┘    └──────────────┘    └──────────────┘       │
│                                                                   │
│  2. SERVING PIPELINE                                             │
│     ┌──────────┐    ┌──────────────┐    ┌──────────────┐       │
│     │   GCS    │───▶│  Dataproc    │───▶│    Kafka     │       │
│     │  Bucket  │    │Spark Streaming│   │ (Predictions)│       │
│     │ (script) │    │    + MLflow   │    │              │       │
│     └──────────┘    └──────────────┘    └──────────────┘       │
│          ▲                                       │               │
│          │          ┌──────────────┐            │               │
│          └──────────│    Kafka     │◀───────────┘               │
│                     │  (Input Data)│                            │
│                     └──────────────┘                            │
└─────────────────────────────────────────────────────────────────┘
```

---

### 1. Training Pipeline

#### Overview
The training pipeline trains multiple sentiment analysis models (LogisticRegression, RandomForest, GBT, LinearSVC) using Spark ML on Dataproc, evaluates their performance on a balanced dataset, and registers the best model to MLflow Model Registry.

#### Script
- **Location**: `code/sentiment_multimodel.py`
- **Purpose**: Multi-model training with automated evaluation and MLflow tracking

#### Training Process

##### Step 1: Upload Training Script to GCS

```bash
# Upload the training script to your GCS bucket
gsutil cp code/sentiment_multimodel.py gs://<YOUR-BUCKET>/code/

# Verify upload
gsutil ls gs://<YOUR-BUCKET>/code/sentiment_multimodel.py
```

##### Step 2: Submit Spark Training Job

```bash
# Basic training job submission
gcloud dataproc jobs submit pyspark \
  gs://<YOUR-BUCKET>/code/sentiment_multimodel.py \
  --cluster=<CLUSTER-NAME> \
  --region=<REGION> \
  --properties="spark.executor.memory=8g,spark.executor.cores=4" \
  -- \
  --input gs://<YOUR-BUCKET>/data_raw/sentiment140/training.1600000.processed.noemoticon.csv \
  --output gs://<YOUR-BUCKET>/models/ \
  --mlflow-uri http://<MLFLOW-SERVER-IP>:5000 \
  --mlflow-experiment sentiment-twitter-experiment \
  --registered-model-name twitter-sentiment-model

# Advanced: with optimization flags
gcloud dataproc jobs submit pyspark \
  gs://<YOUR-BUCKET>/code/sentiment_multimodel.py \
  --cluster=<CLUSTER-NAME> \
  --region=<REGION> \
  --properties="spark.executor.memory=8g,spark.executor.cores=4,spark.sql.shuffle.partitions=200" \
  -- \
  --input gs://<YOUR-BUCKET>/data_raw/sentiment140/training.1600000.processed.noemoticon.csv \
  --output gs://<YOUR-BUCKET>/models/ \
  --mlflow-uri http://<MLFLOW-SERVER-IP>:5000 \
  --mlflow-experiment sentiment-twitter-experiment \
  --registered-model-name twitter-sentiment-model \
  --num-features 32768 \
  --train-ratio 0.8 \
  --val-ratio 0.1 \
  --sample-fraction 0.5 \
  --enable-caching \
  --enable-cv \
  --cv-folds 2
```

##### Step 3: Model Registration

The script automatically:
1. Trains multiple models (LogisticRegression, RandomForest, GBT, LinearSVC)
2. Evaluates each model using accuracy as the primary metric (balanced dataset)
3. Logs all models to MLflow with metrics, parameters, and confusion matrices
4. Registers each model as:
   - `twitter-sentiment-model-logisticregression`
   - `twitter-sentiment-model-randomforest`
   - `twitter-sentiment-model-gbt`
   - `twitter-sentiment-model-linearsvc`
5. Saves the best model as `twitter-sentiment-model-best`

##### Step 4: Verify Model in MLflow

```bash
# Access MLflow UI
http://<MLFLOW-SERVER-IP>:5000

# Check registered models via API
curl http://<MLFLOW-SERVER-IP>:5000/api/2.0/mlflow/registered-models/list
```

#### Training Script Arguments

| Argument | Required | Default | Description |
|----------|----------|---------|-------------|
| `--input` | Yes | - | Input CSV path (GCS or local) |
| `--output` | Yes | - | Output base path for artifacts |
| `--mlflow-uri` | No | None | MLflow tracking server URI |
| `--mlflow-experiment` | No | `default` | MLflow experiment name |
| `--registered-model-name` | No | `twitter-sentiment-model` | Model name prefix in registry |
| `--num-features` | No | 32768 | HashingTF feature dimension |
| `--train-ratio` | No | 0.8 | Training data ratio |
| `--val-ratio` | No | 0.1 | Validation data ratio |
| `--sample-fraction` | No | 1.0 | Fraction of data to use |
| `--enable-cv` | No | False | Enable cross-validation |
| `--cv-folds` | No | 2 | CV folds count |
| `--enable-caching` | No | False | Enable DataFrame caching |
| `--max-rows` | No | 0 | Limit rows for debugging |

#### Outputs

1. **MLflow Artifacts**:
   - Model artifacts for each trained model
   - Confusion matrix plots
   - Training metrics and parameters
   - Best model artifact

2. **GCS Storage**:
   - Model files in `gs://<BUCKET>/models/`
   - Training logs and metadata

3. **MLflow Model Registry**:
   - Registered models with versions
   - Model metadata and lineage
   - Evaluation metrics

---

### 2. Serving Pipeline (Real-time Predictions)

#### Overview
The serving pipeline uses Spark Structured Streaming to load models from MLflow and perform real-time predictions on streaming data from Kafka. It supports hot-swapping models without restarting the stream.

#### Script
- **Location**: `code/streaming_serving_mlflow_hdfs_kafka.py`
- **Purpose**: Real-time model serving with Kafka integration

#### Serving Process

##### Step 1: Upload Serving Script to GCS

```bash
# Upload the serving script
gsutil cp code/streaming_serving_mlflow_hdfs_kafka.py gs://<YOUR-BUCKET>/code/

# Verify upload
gsutil ls gs://<YOUR-BUCKET>/code/streaming_serving_mlflow_hdfs_kafka.py
```

##### Step 2: Set Up Kafka Topics

```bash
# Create input topic for incoming text data
kafka-topics.sh --create \
  --topic sentiment-input \
  --bootstrap-server <KAFKA-BROKER>:9092 \
  --partitions 3 \
  --replication-factor 1

# Create output topic for predictions
kafka-topics.sh --create \
  --topic sentiment-predictions \
  --bootstrap-server <KAFKA-BROKER>:9092 \
  --partitions 3 \
  --replication-factor 1

# Verify topics
kafka-topics.sh --list --bootstrap-server <KAFKA-BROKER>:9092
```

##### Step 3: Submit Spark Streaming Job

```bash
# Kafka source with Kafka sink (recommended for production)
gcloud dataproc jobs submit pyspark \
  gs://<YOUR-BUCKET>/code/streaming_serving_mlflow_hdfs_kafka.py \
  --cluster=<CLUSTER-NAME> \
  --region=<REGION> \
  --properties="spark.executor.memory=8g,spark.executor.cores=4" \
  --jars=gs://spark-lib/bigquery/spark-bigquery-latest_2.12.jar \
  --packages=org.apache.spark:spark-sql-kafka-0-10_2.12:3.3.0 \
  -- \
  --source-type kafka \
  --kafka-bootstrap <KAFKA-BROKER>:9092 \
  --kafka-topic sentiment-input \
  --output-path gs://<YOUR-BUCKET>/predictions/ \
  --checkpoint gs://<YOUR-BUCKET>/checkpoints/streaming \
  --mlflow-uri http://<MLFLOW-SERVER-IP>:5000 \
  --model-name twitter-sentiment-model-best \
  --model-stage Production \
  --model-flavor spark \
  --batch-interval-seconds 30 \
  --reload-interval-seconds 300 \
  --write-kafka true \
  --kafka-output-bootstrap <KAFKA-BROKER>:9092 \
  --kafka-output-topic sentiment-predictions

# Alternative: File source (HDFS/GCS directory watch)
gcloud dataproc jobs submit pyspark \
  gs://<YOUR-BUCKET>/code/streaming_serving_mlflow_hdfs_kafka.py \
  --cluster=<CLUSTER-NAME> \
  --region=<REGION> \
  --properties="spark.executor.memory=8g,spark.executor.cores=4" \
  -- \
  --source-type file \
  --input-path gs://<YOUR-BUCKET>/streaming-input/ \
  --max-files-per-trigger 10 \
  --output-path gs://<YOUR-BUCKET>/predictions/ \
  --checkpoint gs://<YOUR-BUCKET>/checkpoints/streaming \
  --mlflow-uri http://<MLFLOW-SERVER-IP>:5000 \
  --model-name twitter-sentiment-model-best \
  --model-flavor spark \
  --batch-interval-seconds 30 \
  --reload-interval-seconds 300
```

##### Step 4: Send Data via Kafka Producer

```bash
# Python Kafka producer example
python3 << 'EOF'
from kafka import KafkaProducer
import json
import time

producer = KafkaProducer(
    bootstrap_servers='<KAFKA-BROKER>:9092',
    value_serializer=lambda v: json.dumps(v).encode('utf-8')
)

# Send test messages
messages = [
    {"text": "I love this product! It's amazing!"},
    {"text": "Terrible experience, worst service ever."},
    {"text": "Not bad, could be better."}
]

for msg in messages:
    producer.send('sentiment-input', msg)
    print(f"Sent: {msg}")
    time.sleep(1)

producer.flush()
producer.close()
EOF
```

##### Step 5: Consume Predictions from Kafka

```bash
# Console consumer for testing
kafka-console-consumer.sh \
  --topic sentiment-predictions \
  --bootstrap-server <KAFKA-BROKER>:9092 \
  --from-beginning

# Python Kafka consumer example
python3 << 'EOF'
from kafka import KafkaConsumer
import json

consumer = KafkaConsumer(
    'sentiment-predictions',
    bootstrap_servers='<KAFKA-BROKER>:9092',
    value_deserializer=lambda v: json.loads(v.decode('utf-8')),
    auto_offset_reset='earliest'
)

print("Listening for predictions...")
for message in consumer:
    prediction = message.value
    print(f"Prediction: {prediction}")
EOF
```

##### Step 6: Monitor Predictions in GCS

```bash
# List prediction partitions
gsutil ls gs://<YOUR-BUCKET>/predictions/

# Read a sample prediction file
gsutil cat gs://<YOUR-BUCKET>/predictions/date=2025-12-14/*.parquet | head

# Query predictions with BigQuery (if exported)
bq query --use_legacy_sql=false \
'SELECT text, prediction, date 
 FROM `<PROJECT>.<DATASET>.predictions` 
 LIMIT 10'
```

#### Serving Script Arguments

| Argument | Required | Default | Description |
|----------|----------|---------|-------------|
| `--source-type` | Yes | - | Input source: `file` or `kafka` |
| `--input-path` | For file | - | HDFS/GCS directory to watch |
| `--kafka-bootstrap` | For kafka | - | Kafka broker address |
| `--kafka-topic` | For kafka | - | Input Kafka topic |
| `--output-path` | Yes | - | HDFS/GCS output path for predictions |
| `--checkpoint` | Yes | - | Checkpoint location |
| `--mlflow-uri` | Yes | - | MLflow tracking server URI |
| `--model-name` | Yes | - | Registered model name |
| `--model-stage` | No | `Production` | Model stage (Production/Staging) |
| `--model-flavor` | No | `spark` | Model flavor: `spark` or `pyfunc` |
| `--batch-interval-seconds` | No | 30 | Micro-batch interval |
| `--reload-interval-seconds` | No | 300 | Model reload check interval |
| `--write-kafka` | No | `false` | Write predictions to Kafka |
| `--kafka-output-bootstrap` | No | - | Output Kafka broker |
| `--kafka-output-topic` | No | - | Output Kafka topic |
| `--write-gcs` | No | `false` | Write JSON to GCS |

#### Features

1. **Hot Model Swapping**: Automatically detects and loads new model versions from MLflow every 5 minutes (configurable)
2. **Dual Input Sources**: Supports both Kafka streams and file-based (HDFS/GCS) input
3. **Dual Output Sinks**: Writes predictions to both Parquet files (partitioned by date) and Kafka topics
4. **Error Handling**: Robust error handling with batch-level retry logic
5. **Monitoring**: Logs batch-level metrics to MLflow for observability

---

### CI/CD Integration Guide

#### Prerequisites

1. **GCS Bucket**: Store scripts, models, and data
2. **Dataproc Cluster**: Running cluster with Spark and MLflow client
3. **MLflow Server**: Tracking server accessible from Dataproc
4. **Kafka Cluster**: For real-time streaming (optional)
5. **Service Account**: With permissions for GCS, Dataproc, and BigQuery

#### CI/CD Pipeline Structure

```yaml
# Example: .github/workflows/ml-pipeline.yml or Cloud Build configuration

stages:
  - test        # Unit tests and code quality
  - train       # Model training on Dataproc
  - validate    # Model validation and promotion
  - deploy      # Deploy streaming serving job
  - monitor     # Monitor model performance

jobs:
  test:
    - Run pytest on training/serving scripts
    - Validate data schemas
    - Check code quality (pylint, black)

  train:
    - Upload scripts to GCS
    - Submit training job to Dataproc
    - Wait for completion
    - Validate MLflow artifacts

  validate:
    - Load model from MLflow
    - Run validation dataset
    - Compare metrics with baseline
    - Promote to Production stage if passing

  deploy:
    - Upload serving script to GCS
    - Submit streaming job to Dataproc
    - Verify Kafka connectivity
    - Health check predictions

  monitor:
    - Set up alerting on prediction metrics
    - Monitor data drift
    - Track model performance degradation
```

#### Automated Training Job

```bash
#!/bin/bash
# ci-cd/train-model.sh

set -e

PROJECT_ID="<YOUR-PROJECT>"
BUCKET="<YOUR-BUCKET>"
CLUSTER="<CLUSTER-NAME>"
REGION="<REGION>"
MLFLOW_URI="http://<MLFLOW-SERVER-IP>:5000"

# Upload training script
echo "Uploading training script..."
gsutil cp code/sentiment_multimodel.py gs://${BUCKET}/code/

# Submit training job
echo "Submitting training job..."
JOB_ID=$(gcloud dataproc jobs submit pyspark \
  gs://${BUCKET}/code/sentiment_multimodel.py \
  --cluster=${CLUSTER} \
  --region=${REGION} \
  --properties="spark.executor.memory=8g,spark.executor.cores=4" \
  -- \
  --input gs://${BUCKET}/data_raw/sentiment140/training.1600000.processed.noemoticon.csv \
  --output gs://${BUCKET}/models/ \
  --mlflow-uri ${MLFLOW_URI} \
  --mlflow-experiment sentiment-twitter-experiment \
  --registered-model-name twitter-sentiment-model \
  --enable-caching \
  | grep -oP 'Job \[\K[^\]]+')

echo "Training job submitted: ${JOB_ID}"

# Wait for completion
echo "Waiting for training job to complete..."
gcloud dataproc jobs wait ${JOB_ID} --region=${REGION}

echo "Training complete. Check MLflow: ${MLFLOW_URI}"
```

#### Automated Serving Deployment

```bash
#!/bin/bash
# ci-cd/deploy-serving.sh

set -e

PROJECT_ID="<YOUR-PROJECT>"
BUCKET="<YOUR-BUCKET>"
CLUSTER="<CLUSTER-NAME>"
REGION="<REGION>"
MLFLOW_URI="http://<MLFLOW-SERVER-IP>:5000"
KAFKA_BROKER="<KAFKA-BROKER>:9092"

# Upload serving script
echo "Uploading serving script..."
gsutil cp code/streaming_serving_mlflow_hdfs_kafka.py gs://${BUCKET}/code/

# Submit streaming job
echo "Submitting streaming serving job..."
gcloud dataproc jobs submit pyspark \
  gs://${BUCKET}/code/streaming_serving_mlflow_hdfs_kafka.py \
  --cluster=${CLUSTER} \
  --region=${REGION} \
  --properties="spark.executor.memory=8g,spark.executor.cores=4" \
  --packages=org.apache.spark:spark-sql-kafka-0-10_2.12:3.3.0 \
  -- \
  --source-type kafka \
  --kafka-bootstrap ${KAFKA_BROKER} \
  --kafka-topic sentiment-input \
  --output-path gs://${BUCKET}/predictions/ \
  --checkpoint gs://${BUCKET}/checkpoints/streaming \
  --mlflow-uri ${MLFLOW_URI} \
  --model-name twitter-sentiment-model-best \
  --model-stage Production \
  --model-flavor spark \
  --batch-interval-seconds 30 \
  --reload-interval-seconds 300 \
  --write-kafka true \
  --kafka-output-bootstrap ${KAFKA_BROKER} \
  --kafka-output-topic sentiment-predictions

echo "Streaming job deployed and running"
```

#### Model Promotion Script

```python
#!/usr/bin/env python3
# ci-cd/promote-model.py

import mlflow
from mlflow.tracking import MlflowClient
import sys

MLFLOW_URI = "http://<MLFLOW-SERVER-IP>:5000"
MODEL_NAME = "twitter-sentiment-model-best"
MIN_ACCURACY = 0.75  # Minimum accuracy threshold

mlflow.set_tracking_uri(MLFLOW_URI)
client = MlflowClient()

# Get latest model version
versions = client.get_latest_versions(MODEL_NAME)
if not versions:
    print(f"No versions found for {MODEL_NAME}")
    sys.exit(1)

latest_version = max(versions, key=lambda v: int(v.version))
print(f"Latest version: {latest_version.version}")

# Get model metrics from run
run = client.get_run(latest_version.run_id)
accuracy = run.data.metrics.get("accuracy", 0)

print(f"Model accuracy: {accuracy:.4f}")

if accuracy >= MIN_ACCURACY:
    # Transition to Production
    client.transition_model_version_stage(
        name=MODEL_NAME,
        version=latest_version.version,
        stage="Production",
        archive_existing_versions=True
    )
    print(f"✅ Model version {latest_version.version} promoted to Production")
else:
    print(f"❌ Model accuracy {accuracy:.4f} below threshold {MIN_ACCURACY}")
    sys.exit(1)
```

#### Monitoring and Alerting

```bash
# Set up monitoring dashboard queries

# 1. Prediction throughput
SELECT 
  TIMESTAMP_TRUNC(ingest_time, HOUR) as hour,
  COUNT(*) as prediction_count,
  AVG(CAST(prediction AS FLOAT64)) as avg_sentiment
FROM `<PROJECT>.<DATASET>.predictions`
WHERE DATE(ingest_time) = CURRENT_DATE()
GROUP BY hour
ORDER BY hour DESC

# 2. Model performance (if labels available)
SELECT
  date,
  COUNT(*) as total_predictions,
  SUM(CAST(prediction = label AS INT64)) / COUNT(*) as accuracy
FROM `<PROJECT>.<DATASET>.predictions`
WHERE label IS NOT NULL
GROUP BY date
ORDER BY date DESC

# 3. Data distribution monitoring
SELECT
  prediction,
  COUNT(*) as count,
  COUNT(*) / SUM(COUNT(*)) OVER() as percentage
FROM `<PROJECT>.<DATASET>.predictions`
WHERE DATE(ingest_time) = CURRENT_DATE()
GROUP BY prediction
```

---

### Troubleshooting

#### Training Issues

**Problem**: Out of memory errors during training
```bash
# Solution: Reduce data or increase executor memory
--sample-fraction 0.5 \
--properties="spark.executor.memory=16g"
```

**Problem**: Model not appearing in MLflow
```bash
# Check MLflow connectivity
curl http://<MLFLOW-SERVER-IP>:5000/health

# Check training logs
gcloud dataproc jobs describe <JOB-ID> --region=<REGION>
```

#### Serving Issues

**Problem**: Streaming job not receiving data
```bash
# Verify Kafka connectivity
kafka-console-consumer.sh --topic sentiment-input \
  --bootstrap-server <KAFKA-BROKER>:9092 --from-beginning

# Check streaming logs
gcloud dataproc jobs describe <JOB-ID> --region=<REGION>
```

**Problem**: Model hot-swap not working
```bash
# Verify model version in MLflow
curl http://<MLFLOW-SERVER-IP>:5000/api/2.0/mlflow/registered-models/get?name=<MODEL-NAME>

# Check reload interval (default 300s)
--reload-interval-seconds 60  # Reduce to 1 minute for testing
```

---

### Best Practices

1. **Version Control**: Tag all script versions in Git before uploading to GCS
2. **Model Versioning**: Use semantic versioning for model names (e.g., `v1.2.3`)
3. **Data Validation**: Validate input data schema before training
4. **Monitoring**: Set up alerts on prediction latency and accuracy degradation
5. **Checkpointing**: Always use checkpointing for streaming jobs to enable exactly-once processing
6. **Resource Management**: Right-size Spark executors based on data volume
7. **Security**: Use service accounts with minimal required permissions
8. **Cost Optimization**: Use preemptible VMs for training, standard VMs for serving

---

### Next Steps for CI/CD

1. **Containerize Scripts**: Package scripts with dependencies using Docker
2. **Automate Testing**: Set up unit tests and integration tests
3. **GitOps Workflow**: Trigger pipeline on Git commits to main branch
4. **Blue-Green Deployment**: Run parallel serving jobs for zero-downtime updates
5. **A/B Testing**: Deploy multiple model versions and compare performance
6. **Feature Store**: Implement feature store for consistent feature engineering
7. **Model Monitoring**: Set up MLflow metrics collection and alerting
8. **Automated Retraining**: Schedule periodic retraining jobs with Cloud Scheduler

---
