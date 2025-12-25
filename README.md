# Sentiment Analysis MLOps Pipeline

Production-ready end-to-end sentiment analysis ML pipeline with Apache Airflow orchestration, Spark ML training on GCP Dataproc, MLflow experiment tracking, and real-time serving with Spark Structured Streaming.

## 🚀 Features

- **Automated Orchestration**: Apache Airflow DAGs for training and serving workflows
- **Distributed Training**: Multi-model training (LogisticRegression, RandomForest, LinearSVC) on GCP Dataproc
- **Experiment Tracking**: MLflow for model registry, versioning, and metrics tracking
- **Real-time Serving**: Spark Structured Streaming with hot-swappable models
- **Dual Input Sources**: File-based (GCS) and Kafka stream processing
- **Model Hot-Swap**: Automatic model reloading without service downtime
- **Cross-validation**: Hyperparameter tuning with configurable CV folds
- **Production Ready**: Complete monitoring, error handling, and CI/CD integration

## 📊 Architecture Overview

```
┌─────────────────────────────────────────────────────────────────────┐
│                      Complete Pipeline Architecture                  │
├─────────────────────────────────────────────────────────────────────┤
│                                                                       │
│  TRAINING PIPELINE (Weekly / On-Demand)                             │
│  ┌──────────┐    ┌──────────────┐    ┌──────────────┐             │
│  │ Airflow  │───▶│  Dataproc    │───▶│   MLflow     │             │
│  │   DAG    │    │ Spark Training│   │   Registry   │             │
│  └──────────┘    └──────────────┘    └──────────────┘             │
│       │                                      │                       │
│       ▼                                      ▼                       │
│  ┌──────────────────────────────────────────────────────┐          │
│  │ GCS Storage: Raw Data, Models, Artifacts             │          │
│  └──────────────────────────────────────────────────────┘          │
│                                                                       │
│  SERVING PIPELINE (Real-time)                                       │
│  ┌──────────┐    ┌──────────────┐    ┌──────────────┐             │
│  │ Kafka/   │───▶│  Dataproc    │───▶│    Kafka     │             │
│  │   GCS    │    │   Streaming  │    │ /GCS Output  │             │
│  │  Input   │    │  + MLflow    │    │              │             │
│  └──────────┘    └──────────────┘    └──────────────┘             │
│                          ▲                                           │
│                          │ Model Hot-Swap (every 5 min)            │
│                          │                                           │
│                   ┌──────┴───────┐                                  │
│                   │    MLflow    │                                  │
│                   │   Registry   │                                  │
│                   └──────────────┘                                  │
└─────────────────────────────────────────────────────────────────────┘
```

## 📁 Project Structure

```
sentiment-project/
├── README.md                              # This comprehensive guide
├── PIPELINE_FLOW_DOCUMENTATION.md         # Detailed pipeline flow diagrams
├── requirements.txt                       # Python dependencies
├── test_dataproc.sh                       # Dataproc connectivity test
│
├── airflow/                               # Airflow orchestration
│   ├── docker-compose.yml                 # Airflow services (webserver, scheduler)
│   ├── Dockerfile                         # Custom Airflow image
│   ├── airflow.cfg                        # Airflow configuration
│   ├── requirements.txt                   # Airflow dependencies
│   ├── setup_local.sh                     # Local setup script
│   ├── start_services.sh                  # Start Airflow services
│   ├── stop_services.sh                   # Stop Airflow services
│   ├── fix_permissions.sh                 # Fix Docker volume permissions
│   ├── README.md                          # Airflow setup guide
│   ├── GCP_DATAPROC_SETUP.md             # Dataproc integration guide
│   └── dags/
│       ├── sentiment_training_pipeline.py          # Main training DAG
│       ├── sentiment_training_local.py             # Local training DAG
│       ├── sentiment_serving_pipeline.py           # Model serving DAG
│       ├── sentiment_serving_management.py         # Serving monitoring DAG
│       └── SERVING_DAGS_README.md                 # Serving documentation
│
├── code/                                  # Core ML scripts
│   ├── sentiment_multimodel.py            # Multi-model training script
│   ├── streaming_serving_mlflow_hdfs_kafka.py           # Basic serving
│   └── streaming_serving_mlflow_hdfs_kafka_optimize.py  # Optimized serving
│
└── tests/                                 # Unit tests
    └── test_streaming_mlflow_utils.py
```

---

## 🛠️ Prerequisites

### Infrastructure Requirements

1. **GCP Project** with the following APIs enabled:
   - Dataproc API
   - Cloud Storage API
   - Compute Engine API
   - Cloud Logging API

2. **GCP Dataproc Cluster**:
   ```bash
   gcloud dataproc clusters create sentiment-cluster \
       --region=us-central1 \
       --zone=us-central1-a \
       --master-machine-type=n1-standard-4 \
       --master-boot-disk-size=100 \
       --num-workers=3 \
       --worker-machine-type=n1-standard-4 \
       --worker-boot-disk-size=100 \
       --image-version=2.1-debian11 \
       --enable-component-gateway \
       --optional-components=JUPYTER \
       --max-idle=3600s \
       --properties=spark:spark.executor.memory=4g,spark:spark.executor.cores=2
   ```

3. **GCS Buckets**:
   ```bash
   # Create required buckets
   gsutil mb gs://sentiment-bucket-code
   gsutil mb gs://sentiment-bucket-raw
   gsutil mb gs://sentiment-bucket-artifacts
   gsutil mb gs://sentiment-bucket-models
   gsutil mb gs://sentiment-bucket-predictions
   gsutil mb gs://sentiment-bucket-checkpoint
   gsutil mb gs://sentiment-bucket-streaming-input
   ```

4. **MLflow Tracking Server**: Running on GCP VM or Cloud Run
   - Accessible at: `http://<MLFLOW_VM_IP>:5000`
   - Configured with artifact storage to GCS

5. **Apache Kafka** (Optional): For real-time streaming
   - Bootstrap servers: `<KAFKA_BROKER>:9092`
   - Topics: `sentiment-input`, `sentiment-predictions`

6. **Service Account** with permissions:
   - Dataproc Worker
   - Storage Object Admin
   - Logging Writer

### Local Requirements

- **Docker & Docker Compose**: For Airflow
- **Python 3.8+**: For local development
- **gcloud CLI**: For GCP interaction
- **kaggle CLI**: For dataset download (optional)

---

## ⚡ Quick Start

### 1. Download Sentiment140 Dataset

```bash
# Set up Kaggle credentials
export KAGGLE_CONFIG_DIR=$HOME/.kaggle
# Place kaggle.json in ~/.kaggle/

# Download and upload to GCS
python code/download_and_upload.py \
  --dataset kazanova/sentiment140 \
  --gcs-bucket sentiment-bucket-raw \
  --output-dir data_raw/sentiment140 \
  --gcs-prefix data_raw/sentiment140 \
  --unzip

# Verify upload
gsutil ls gs://sentiment-bucket-raw/data_raw/sentiment140/
```

### 2. Set Up Airflow

```bash
cd sentiment-project/airflow

# Fix permissions (Linux/Mac)
./fix_permissions.sh

# Start Airflow services
./start_services.sh

# Access Airflow UI
# http://localhost:8080
# Username: airflow
# Password: airflow
```

### 3. Configure GCP Credentials in Airflow

```bash
# Copy service account key
cp /path/to/your-service-account-key.json ./airflow/gcp-credentials.json

# Update docker-compose.yml (already configured):
# - ./gcp-credentials.json:/opt/airflow/gcp-credentials.json:ro

# Create Google Cloud connection
docker-compose exec airflow-webserver airflow connections add 'google_cloud_default' \
    --conn-type 'google_cloud_platform' \
    --conn-extra '{
      "keyfile_path": "/opt/airflow/gcp-credentials.json",
      "project": "YOUR_PROJECT_ID",
      "scope": "https://www.googleapis.com/auth/cloud-platform"
    }'
```

### 4. Set Airflow Variables

```bash
# Training variables
docker-compose exec airflow-webserver airflow variables set GCP_PROJECT_ID "your-project-id"
docker-compose exec airflow-webserver airflow variables set GCP_REGION "us-central1"
docker-compose exec airflow-webserver airflow variables set DATAPROC_CLUSTER_NAME "sentiment-cluster"
docker-compose exec airflow-webserver airflow variables set GCS_BUCKET "sentiment-bucket"
docker-compose exec airflow-webserver airflow variables set MLFLOW_VM_IP "34.66.73.187"
docker-compose exec airflow-webserver airflow variables set MLFLOW_TRACKING_URI "http://34.66.73.187:5000"

# Serving variables
docker-compose exec airflow-webserver airflow variables set serving_deploy_mode "test"
docker-compose exec airflow-webserver airflow variables set serving_model_name "twitter-sentiment-model-best"
docker-compose exec airflow-webserver airflow variables set serving_model_stage "Production"
docker-compose exec airflow-webserver airflow variables set serving_source_type "file"
docker-compose exec airflow-webserver airflow variables set serving_batch_interval "30"
docker-compose exec airflow-webserver airflow variables set serving_reload_interval "300"

# Kafka variables (if using Kafka source)
docker-compose exec airflow-webserver airflow variables set kafka_bootstrap_servers "your-kafka-server:9092"
docker-compose exec airflow-webserver airflow variables set kafka_input_topic "sentiment-input"
docker-compose exec airflow-webserver airflow variables set kafka_output_topic "sentiment-predictions"
```

### 5. Upload Training Scripts to GCS

```bash
gsutil cp code/sentiment_multimodel.py gs://sentiment-bucket-code/
gsutil cp code/streaming_serving_mlflow_hdfs_kafka_optimize.py gs://sentiment-bucket-code/
```

### 6. Trigger Training Pipeline

```bash
# Via Airflow UI
# 1. Go to http://localhost:8080
# 2. Enable DAG: sentiment_ml_training_pipeline
# 3. Click "Trigger DAG" button

# Or via CLI
docker-compose exec airflow-webserver airflow dags trigger sentiment_ml_training_pipeline
```

---

## 📚 Training Pipeline

### Overview

The training pipeline (`sentiment_ml_training_pipeline`) orchestrates the complete ML workflow from data validation to model registration.

```
┌─────────────────────────────────────────────────────────────────────┐
│ 1. AIRFLOW DAG TRIGGER                                              │
│    - Manual or scheduled (@weekly)                                  │
│    - DAG: sentiment_ml_training_pipeline                            │
└────────────────┬────────────────────────────────────────────────────┘
                 │
                 ▼
┌─────────────────────────────────────────────────────────────────────┐
│ 2. VALIDATION & PREPARATION                                         │
│    - validate_data: Check CSV exists (~1.6M rows)                   │
│    - check_mlflow_connection: Verify MLflow at 34.66.73.187:5000   │
│    - prepare_environment: Create output directories                 │
└────────────────┬────────────────────────────────────────────────────┘
                 │
                 ▼
┌─────────────────────────────────────────────────────────────────────┐
│ 3. SUBMIT TO DATAPROC                                               │
│    Cluster: sentiment-cluster (us-central1)                         │
│    ┌───────────────────────────┐  ┌──────────────────────────────┐ │
│    │ Job 1: Full Training      │  │ Job 2: CV Training           │ │
│    │ - 100% data               │  │ - 30% data sample            │ │
│    │ - 3 models                │  │ - 2-fold CV                  │ │
│    │ - No CV                   │  │ - Hyperparameter tuning      │ │
│    └───────────────────────────┘  └──────────────────────────────┘ │
└────────────────┬────────────────────────────────────────────────────┘
                 │
                 ▼
┌─────────────────────────────────────────────────────────────────────┐
│ 4. SPARK TRAINING JOB (sentiment_multimodel.py)                     │
│    ┌──────────────────────────────────────────────────────────────┐│
│    │ Data Prep: Read CSV → Transform → Split (80/10/10)          ││
│    └──────────────────────────────────────────────────────────────┘│
│    ┌──────────────────────────────────────────────────────────────┐│
│    │ Feature Pipeline: Tokenizer → StopWords → HashingTF → IDF   ││
│    └──────────────────────────────────────────────────────────────┘│
│    ┌──────────────────────────────────────────────────────────────┐│
│    │ Train Models (Parallel):                                     ││
│    │   • Logistic Regression                                      ││
│    │   • Random Forest                                            ││
│    │   • Linear SVC                                               ││
│    └──────────────────────────────────────────────────────────────┘│
└────────────────┬────────────────────────────────────────────────────┘
                 │
                 ▼
┌─────────────────────────────────────────────────────────────────────┐
│ 5. MLFLOW REGISTRATION                                              │
│    MLflow Server: http://34.66.73.187:5000                          │
│    ┌──────────────────────────────────────────────────────────────┐│
│    │ Parent Run: sentiment_multi_model_experiment                 ││
│    │   ├─ Child Run: LogisticRegression                          ││
│    │   │   - Log metrics: accuracy, auc, f1, precision, recall   ││
│    │   │   - Log params: regParam, elasticNetParam               ││
│    │   │   - Log artifacts: confusion_matrix.png                 ││
│    │   │   - Register: twitter-sentiment-model-logisticregression││
│    │   │                                                           ││
│    │   ├─ Child Run: RandomForest                                ││
│    │   │   - Register: twitter-sentiment-model-randomforest      ││
│    │   │                                                           ││
│    │   └─ Child Run: LinearSVC                                   ││
│    │       - Register: twitter-sentiment-model-linearsvc         ││
│    │                                                               ││
│    │   ├─ Register Best: twitter-sentiment-model-best            ││
│    │   └─ Save to GCS: gs://bucket/models/best-model-*           ││
│    └──────────────────────────────────────────────────────────────┘│
└────────────────┬────────────────────────────────────────────────────┘
                 │
                 ▼
┌─────────────────────────────────────────────────────────────────────┐
│ 6. GET BEST MODEL (Back in Airflow)                                 │
│    Task: get_best_model_info                                        │
│    - Query MLflow for highest accuracy run                          │
│    - Extract: run_id, accuracy, auc, model_type                     │
│    - Push to XCom                                                   │
│    - Log completion notification                                    │
└────────────────┬────────────────────────────────────────────────────┘
                 │
                 ▼
┌─────────────────────────────────────────────────────────────────────┐
│ 7. MODEL SERVING (streaming_serving_mlflow_hdfs_kafka_optimize.py) │
│    ┌──────────────────────────────────────────────────────────────┐│
│    │ Input Sources:                                               ││
│    │  • File: HDFS/GCS directory watch                            ││
│    │  • Kafka: Real-time stream                                   ││
│    └──────────────────────────────────────────────────────────────┘│
│    ┌──────────────────────────────────────────────────────────────┐│
│    │ Model Loading:                                               ││
│    │  • Load from MLflow: models:/twitter-sentiment-model-best/v ││
│    │  • Cache in driver memory                                    ││
│    │  • Hot-swap: Check every 300s for new version                ││
│    └──────────────────────────────────────────────────────────────┘│
│    ┌──────────────────────────────────────────────────────────────┐│
│    │ Prediction (foreachBatch):                                   ││
│    │  1. Load/reload model if needed                              ││
│    │  2. model.transform(batch_df)                                ││
│    │  3. Add metadata: timestamp, model_version, date             ││
│    │  4. Write to sinks                                           ││
│    └──────────────────────────────────────────────────────────────┘│
│    ┌──────────────────────────────────────────────────────────────┐│
│    │ Output Sinks:                                                ││
│    │  • Parquet: gs://bucket/predictions/ (partitioned by date)   ││
│    │  • Kafka: Optional real-time output topic                    ││
│    └──────────────────────────────────────────────────────────────┘│
└─────────────────────────────────────────────────────────────────────┘

### DAG Flow

```
start → validate_data → check_mlflow_connection → prepare_environment
   ↓
model_training [Parallel Tasks]
   ├─ train_models_full_dataset (100% data, no CV)
   └─ train_models_with_cv (30% data, 2-fold CV)
   ↓
get_best_model_info → generate_training_report → send_completion_notification → end
```

### Training Process

#### Step 1: Data Validation
- Validates input CSV exists at GCS path
- Checks row count (~1.6M rows for Sentiment140)
- Pushes metadata to XCom

#### Step 2: MLflow Connection Check
- Verifies MLflow tracking server accessibility
- Creates experiment if doesn't exist
- Confirms connection to `http://<MLFLOW_VM_IP>:5000`

#### Step 3: Parallel Training Jobs

**Job 1: Full Dataset Training**
```bash
# Trains on 100% of data without CV for production models
Arguments:
  --input gs://sentiment-bucket-raw/training.1600000.processed.noemoticon.csv
  --output gs://sentiment-bucket-models/
  --mlflow-uri http://34.66.73.187:5000
  --mlflow-experiment sentiment-analysis-pipeline
  --sample-fraction 1.0
  --num-features 32768
```

**Job 2: Cross-Validation Training**
```bash
# Trains on 30% sample with hyperparameter tuning
Arguments:
  --enable-cv
  --cv-folds 2
  --sample-fraction 0.3
  --num-features 16384
  --mlflow-experiment sentiment-analysis-pipeline-cv
```

#### Step 4: Model Training Details

**Script**: `code/sentiment_multimodel.py`

**Models Trained**:
1. **Logistic Regression**
   - Parameters: `maxIter=20`, `regParam=0.1`
   - CV Grid: `regParam=[0.01, 0.1]`, `elasticNetParam=[0.0, 0.5]`

2. **Random Forest Classifier**
   - Parameters: `numTrees=10`, `maxDepth=5`, `maxBins=32`
   - CV Grid: `numTrees=[5, 10]`, `maxDepth=[3, 5]`

3. **Linear SVC**
   - Parameters: `maxIter=30`, `regParam=0.1`
   - CV Grid: `regParam=[0.01, 0.1]`, `maxIter=[20, 30]`

**Feature Pipeline**:
```python
1. Tokenizer: Split text into tokens
2. StopWordsRemover: Remove common words
3. HashingTF: Convert tokens to features (32768 dimensions)
4. IDF: Apply inverse document frequency weighting
```

**Data Split**:
- Training: 80%
- Validation: 10%
- Test: 10%

#### Step 5: MLflow Registration

**Registered Models**:
- `twitter-sentiment-model-logisticregression`
- `twitter-sentiment-model-randomforest`
- `twitter-sentiment-model-linearsvc`
- `twitter-sentiment-model-best` (highest accuracy)

**Metrics Logged**:
- Accuracy
- AUC (Area Under ROC Curve)
- F1 Score
- Precision
- Recall

**Artifacts**:
- Confusion matrix PNG
- Model files
- Feature pipeline

#### Step 6: Model Selection

Best model is selected based on **highest test accuracy** and registered as `twitter-sentiment-model-best`.

### Training Arguments Reference

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

### Monitoring Training

**Airflow UI**: `http://localhost:8080`
- View DAG runs, task logs, execution times
- Check XCom data for model metrics

**MLflow UI**: `http://<MLFLOW_VM_IP>:5000`
- Compare experiment runs
- View metrics charts
- Download model artifacts

**Dataproc Console**: GCP Console → Dataproc → Jobs
- Monitor job status
- View driver/executor logs
- Check resource utilization

---

## 🔮 Serving Pipeline

### Overview

The serving pipeline deploys the best trained model from MLflow to Dataproc for real-time predictions using Spark Structured Streaming.

### Serving DAGs

#### 1. `sentiment_model_serving` - Deploy Model Serving

**Purpose**: Deploy the best trained model for real-time predictions

**Trigger**: Manual only (not scheduled)

**What it does**:
1. ✅ Validates MLflow connection
2. 🔍 Gets the best model from MLflow Registry
3. 🚀 Deploys streaming serving job to Dataproc
4. 📊 Monitors deployment status
5. 📧 Sends deployment notification

**Use this when**: You want to start serving predictions with a trained model

#### 2. `sentiment_serving_management` - Manage Serving Jobs

**Purpose**: Monitor, check status, and manage active serving jobs

**Trigger**: Manual only

**What it does**:
1. 📋 Lists all active serving jobs on Dataproc
2. 📈 Shows job performance metrics
3. 📁 Checks prediction output files
4. 📝 Creates monitoring summary report

**Use this when**: You want to check if serving is working correctly or troubleshoot issues

### Deployment Steps

#### Step 1: Configure Serving Variables

```bash
# Access Airflow container
docker-compose exec airflow-webserver bash

# Set serving variables
airflow variables set serving_deploy_mode "test"  # or "production"
airflow variables set serving_model_name "twitter-sentiment-model-best"
airflow variables set serving_model_stage "Production"
airflow variables set serving_source_type "file"  # or "kafka"
airflow variables set serving_batch_interval "30"
airflow variables set serving_reload_interval "300"

# For Kafka source (optional)
airflow variables set kafka_bootstrap_servers "your-kafka-server:9092"
airflow variables set kafka_input_topic "sentiment-input"
airflow variables set kafka_output_topic "sentiment-predictions"
```

#### Step 2: Deploy in Test Mode

1. Open Airflow UI: `http://localhost:8080`
2. Find DAG: `sentiment_model_serving`
3. Click the play button ▶️ to trigger
4. Monitor execution in Graph or Grid view

**Test mode will**:
- ✅ Validate configuration
- ✅ Check model availability
- ❌ NOT actually deploy to Dataproc
- ✅ Show what would be deployed

#### Step 3: Deploy to Production

```bash
# Switch to production mode
airflow variables set serving_deploy_mode "production"

# Ensure GCS paths exist
gsutil mb gs://sentiment-bucket-predictions || true
gsutil mb gs://sentiment-bucket-checkpoint || true
gsutil mb gs://sentiment-bucket-streaming-input || true

# Trigger the DAG via Airflow UI
```

### Serving Modes

#### File Source (Default)

Watches a GCS directory for new CSV files and processes them in real-time.

**Configuration**:
```python
serving_source_type = "file"
# Input: gs://sentiment-bucket-streaming-input/
# Processes files as they arrive
```

**Test it**:
```bash
# Upload a test file
echo "text,label
I love this product!,
This is terrible,
Not bad" > test_input.csv

gsutil cp test_input.csv gs://sentiment-bucket-streaming-input/batch_$(date +%Y%m%d_%H%M%S).csv

# Check predictions
gsutil ls -lh gs://sentiment-bucket-predictions/predictions/
```

#### Kafka Source

Reads messages from Kafka topic in real-time.

**Configuration**:
```python
serving_source_type = "kafka"
kafka_bootstrap_servers = "your-kafka:9092"
kafka_input_topic = "sentiment-input"
kafka_output_topic = "sentiment-predictions"
```

**Message format**:
```json
{
  "text": "I love this product!",
  "id": "12345",
  "timestamp": "2025-12-25T10:00:00Z"
}
```

**Send test messages**:
```python
from kafka import KafkaProducer
import json

producer = KafkaProducer(
    bootstrap_servers='your-kafka:9092',
    value_serializer=lambda v: json.dumps(v).encode('utf-8')
)

messages = [
    {"text": "I love this product! It's amazing!"},
    {"text": "This is the worst experience ever."},
    {"text": "Not bad, could be better."}
]

for msg in messages:
    producer.send('sentiment-input', msg)

producer.flush()
producer.close()
```

### Model Hot-Swap Feature

The serving job automatically checks for new model versions every 5 minutes (configurable).

**How it works**:
1. Serving job loads model v1 from MLflow
2. Training pipeline registers new model v2
3. After 5 minutes, serving detects v2
4. Serving reloads model v2 without stopping
5. New predictions use v2 automatically

**No downtime!** 🎉

### Serving Architecture

```
┌─────────────────────────────────────────────────────────┐
│ Dataproc Cluster: sentiment-cluster                     │
│                                                          │
│  Spark Streaming Job:                                   │
│  streaming_serving_mlflow_hdfs_kafka_optimize.py        │
│                                                          │
│  ┌────────────┐      ┌──────────┐      ┌────────────┐  │
│  │   Input    │  →   │  Model   │  →   │   Output   │  │
│  │ File/Kafka │      │ Predict  │      │ GCS/Kafka  │  │
│  └────────────┘      └──────────┘      └────────────┘  │
│                           ↑                             │
│                           │                             │
│                      ┌────┴─────┐                       │
│                      │  MLflow  │                       │
│                      │ Registry │                       │
│                      └──────────┘                       │
└─────────────────────────────────────────────────────────┘
```

### Serving Script Arguments

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

### Output Structure

**Predictions Output**:
```
gs://sentiment-bucket-predictions/predictions/
├─ date=2025-12-25/
│   ├─ part-00000.parquet
│   ├─ part-00001.parquet
│   └─ _SUCCESS
├─ date=2025-12-26/
│   └─ part-00000.parquet
```

**Schema**:
- `text`: Input text
- `label`: Original label (if provided)
- `features`: Feature vector
- `prediction`: Predicted sentiment (0=negative, 1=positive)
- `probability`: Prediction confidence
- `prediction_timestamp`: When prediction was made
- `model_version`: MLflow model version
- `date`: Partition date

---

## 📊 Monitoring & Troubleshooting

### Check Serving Status

#### Option 1: Use Management DAG
- Trigger `sentiment_serving_management` in Airflow
- View logs for detailed status

#### Option 2: Command Line
```bash
# List running jobs
gcloud dataproc jobs list \
  --cluster=sentiment-cluster \
  --region=us-central1 \
  --filter="status.state=RUNNING"

# Get job details
gcloud dataproc jobs describe JOB_ID \
  --region=us-central1

# View driver logs
gcloud dataproc jobs describe JOB_ID \
  --region=us-central1 \
  --format="value(driverOutputResourceUri)"
```

### Check Predictions

```bash
# List prediction files
gsutil ls -lh gs://sentiment-bucket-predictions/predictions/

# View recent files by date
gsutil ls gs://sentiment-bucket-predictions/predictions/date=*/

# Download a sample
gsutil cp gs://sentiment-bucket-predictions/predictions/date=2025-12-25/part-00000.parquet .

# Read with Python
import pandas as pd
df = pd.read_parquet('part-00000.parquet')
print(df.head())
```

### Common Issues

#### 1. "Model not found in MLflow"
**Solution**: Run training pipeline first (`sentiment_ml_training_pipeline`)
```bash
# Verify in MLflow UI
http://<MLFLOW_VM_IP>:5000
```

#### 2. "Dataproc cluster not found"
**Solution**: Ensure cluster is running
```bash
gcloud dataproc clusters list --region=us-central1
```

#### 3. "No predictions being written"
**Solution**: Check if input files/messages are arriving
```bash
# For file source: Upload test file
gsutil cp test_input.csv gs://sentiment-bucket-streaming-input/

# Check serving job logs in Dataproc
gcloud dataproc jobs describe JOB_ID --region=us-central1
```

#### 4. "Out of memory error"
**Solution**: Reduce batch size or increase executor memory
```bash
# Edit DAG: Increase spark.executor.memory in dataproc_properties
'spark.executor.memory': '16g'
```

### Stopping Serving

#### Stop via GCP Console
1. Go to: https://console.cloud.google.com/dataproc/jobs
2. Find running streaming job
3. Click "Kill" button

#### Stop via Command Line
```bash
# List jobs to get JOB_ID
gcloud dataproc jobs list --cluster=sentiment-cluster --region=us-central1

# Kill the job
gcloud dataproc jobs kill JOB_ID --region=us-central1
```

#### Stop and Clean Checkpoint (Fresh Start)
```bash
# Kill job
gcloud dataproc jobs kill JOB_ID --region=us-central1

# Delete checkpoint (forces fresh start)
gsutil rm -r gs://sentiment-bucket-checkpoint/*

# Redeploy via Airflow DAG
```

---

## ⚙️ Performance Tuning

### Dataproc Cluster Optimization

#### Auto-scaling Configuration
```bash
gcloud dataproc autoscaling-policies create sentiment-autoscaling \
    --region=us-central1 \
    --worker-min-count=2 \
    --worker-max-count=10 \
    --cooldown-period=2m

# Apply to cluster
gcloud dataproc clusters update sentiment-cluster \
    --region=us-central1 \
    --autoscaling-policy=sentiment-autoscaling
```

#### Cost Optimization with Preemptible Workers
```bash
gcloud dataproc clusters create sentiment-cluster \
    --region=us-central1 \
    --num-workers=3 \
    --num-preemptible-workers=5 \
    --max-idle=600s
```

### Spark Configuration

#### For Large Datasets (Training)
```python
dataproc_properties = {
    'spark.executor.memory': '16g',
    'spark.executor.cores': '4',
    'spark.executor.instances': '10',
    'spark.driver.memory': '8g',
    'spark.sql.shuffle.partitions': '400',
    'spark.default.parallelism': '160',
    'spark.dynamicAllocation.enabled': 'true',
    'spark.dynamicAllocation.minExecutors': '2',
    'spark.dynamicAllocation.maxExecutors': '20',
}
```

#### For Streaming (Serving)
```python
dataproc_properties = {
    'spark.executor.memory': '8g',
    'spark.executor.cores': '2',
    'spark.streaming.backpressure.enabled': 'true',
    'spark.sql.streaming.metricsEnabled': 'true',
}
```

### Serving Performance Tuning

#### Adjust Batch Interval
```bash
# Process faster (every 10 seconds)
airflow variables set serving_batch_interval "10"

# Process slower (every 60 seconds) - better for batch workloads
airflow variables set serving_batch_interval "60"
```

#### Adjust Model Reload Interval
```bash
# Check for new models more frequently (every 2 minutes)
airflow variables set serving_reload_interval "120"

# Check less frequently (every 10 minutes)
airflow variables set serving_reload_interval "600"
```

---

## 🔄 CI/CD Integration

### Automated Training Pipeline

```bash
#!/bin/bash
# ci-cd/train-model.sh

set -e

PROJECT_ID="your-project-id"
BUCKET="sentiment-bucket"
CLUSTER="sentiment-cluster"
REGION="us-central1"
MLFLOW_URI="http://34.66.73.187:5000"

# Upload training script
echo "Uploading training script..."
gsutil cp code/sentiment_multimodel.py gs://${BUCKET}-code/

# Submit training job
echo "Submitting training job..."
JOB_ID=$(gcloud dataproc jobs submit pyspark \
  gs://${BUCKET}-code/sentiment_multimodel.py \
  --cluster=${CLUSTER} \
  --region=${REGION} \
  --project=${PROJECT_ID} \
  -- \
  --input gs://${BUCKET}-raw/training.1600000.processed.noemoticon.csv \
  --output gs://${BUCKET}-models/ \
  --mlflow-uri ${MLFLOW_URI} \
  --mlflow-experiment sentiment-analysis-pipeline \
  --registered-model-name twitter-sentiment-model \
  --sample-fraction 1.0 \
  --num-features 32768 \
  | grep -oP 'Job \[\K[^\]]+')

echo "Training job submitted: ${JOB_ID}"

# Wait for completion
echo "Waiting for training job to complete..."
gcloud dataproc jobs wait ${JOB_ID} --region=${REGION}

echo "Training complete. Check MLflow: ${MLFLOW_URI}"
```

### Automated Serving Deployment

```bash
#!/bin/bash
# ci-cd/deploy-serving.sh

set -e

PROJECT_ID="your-project-id"
BUCKET="sentiment-bucket"
CLUSTER="sentiment-cluster"
REGION="us-central1"
MLFLOW_URI="http://34.66.73.187:5000"
KAFKA_BROKER="your-kafka:9092"

# Upload serving script
echo "Uploading serving script..."
gsutil cp code/streaming_serving_mlflow_hdfs_kafka_optimize.py gs://${BUCKET}-code/

# Submit streaming job
echo "Submitting streaming serving job..."
gcloud dataproc jobs submit pyspark \
  gs://${BUCKET}-code/streaming_serving_mlflow_hdfs_kafka_optimize.py \
  --cluster=${CLUSTER} \
  --region=${REGION} \
  --project=${PROJECT_ID} \
  --jars=gs://spark-lib/bigquery/spark-bigquery-latest_2.12.jar \
  --packages=org.apache.spark:spark-sql-kafka-0-10_2.12:3.3.0,org.mlflow:mlflow-spark:2.8.1 \
  -- \
  --source-type kafka \
  --kafka-bootstrap ${KAFKA_BROKER} \
  --kafka-topic sentiment-input \
  --output-path gs://${BUCKET}-predictions/predictions \
  --checkpoint gs://${BUCKET}-checkpoint/streaming \
  --mlflow-uri ${MLFLOW_URI} \
  --model-name twitter-sentiment-model-best \
  --model-stage Production \
  --batch-interval-seconds 30 \
  --reload-interval-seconds 300 \
  --write-kafka true \
  --kafka-output-bootstrap ${KAFKA_BROKER} \
  --kafka-output-topic sentiment-predictions

echo "Streaming job deployed and running"
```

### GitHub Actions Workflow

```yaml
# .github/workflows/ml-pipeline.yml

name: ML Pipeline CI/CD

on:
  push:
    branches: [main]
  pull_request:
    branches: [main]
  schedule:
    - cron: '0 0 * * 0'  # Weekly on Sunday

jobs:
  test:
    runs-on: ubuntu-latest
    steps:
      - uses: actions/checkout@v3
      
      - name: Set up Python
        uses: actions/setup-python@v4
        with:
          python-version: '3.8'
      
      - name: Install dependencies
        run: |
          pip install -r requirements.txt
          pip install pytest flake8
      
      - name: Run linting
        run: flake8 code/ tests/
      
      - name: Run unit tests
        run: pytest tests/

  train:
    needs: test
    runs-on: ubuntu-latest
    if: github.ref == 'refs/heads/main'
    steps:
      - uses: actions/checkout@v3
      
      - name: Set up Cloud SDK
        uses: google-github-actions/setup-gcloud@v1
        with:
          service_account_key: ${{ secrets.GCP_SA_KEY }}
          project_id: ${{ secrets.GCP_PROJECT_ID }}
      
      - name: Upload training script
        run: |
          gsutil cp code/sentiment_multimodel.py gs://${{ secrets.GCS_BUCKET }}-code/
      
      - name: Trigger training job
        run: |
          bash ci-cd/train-model.sh

  deploy:
    needs: train
    runs-on: ubuntu-latest
    if: github.ref == 'refs/heads/main'
    steps:
      - uses: actions/checkout@v3
      
      - name: Set up Cloud SDK
        uses: google-github-actions/setup-gcloud@v1
        with:
          service_account_key: ${{ secrets.GCP_SA_KEY }}
          project_id: ${{ secrets.GCP_PROJECT_ID }}
      
      - name: Deploy serving
        run: |
          bash ci-cd/deploy-serving.sh
```

---

## 🧪 Testing

### Unit Tests

```bash
# Run all tests
pytest tests/

# Run specific test
pytest tests/test_streaming_mlflow_utils.py

# Run with coverage
pytest --cov=code tests/
```

### Integration Tests

```bash
# Test Dataproc connectivity
./test_dataproc.sh

# Test MLflow connection
curl http://<MLFLOW_VM_IP>:5000/health

# Test GCS access
gsutil ls gs://sentiment-bucket-code/
```

### End-to-End Test

```bash
# 1. Upload test data
echo "I love this!,1" > test_small.csv
gsutil cp test_small.csv gs://sentiment-bucket-raw/

# 2. Run training with small dataset
gcloud dataproc jobs submit pyspark \
  gs://sentiment-bucket-code/sentiment_multimodel.py \
  --cluster=sentiment-cluster \
  --region=us-central1 \
  -- \
  --input gs://sentiment-bucket-raw/test_small.csv \
  --output gs://sentiment-bucket-models/test/ \
  --mlflow-uri http://34.66.73.187:5000 \
  --max-rows 1000

# 3. Check model in MLflow
curl http://34.66.73.187:5000/api/2.0/mlflow/registered-models/list

# 4. Test serving with file input
gsutil cp test_small.csv gs://sentiment-bucket-streaming-input/test_$(date +%s).csv

# 5. Wait 1 minute and check predictions
gsutil ls gs://sentiment-bucket-predictions/predictions/
```

---

## 📖 Documentation

- **[PIPELINE_FLOW_DOCUMENTATION.md](sentiment-project/PIPELINE_FLOW_DOCUMENTATION.md)** - Detailed pipeline flow diagrams with step-by-step explanations
- **[airflow/README.md](sentiment-project/airflow/README.md)** - Airflow setup, configuration, and local development guide
- **[airflow/GCP_DATAPROC_SETUP.md](sentiment-project/airflow/GCP_DATAPROC_SETUP.md)** - Complete Dataproc integration guide with performance tuning
- **[airflow/dags/SERVING_DAGS_README.md](sentiment-project/airflow/dags/SERVING_DAGS_README.md)** - Comprehensive serving DAGs documentation with troubleshooting

---

## 🔒 Security Best Practices

### Credentials Management

```bash
# Never commit credentials to git
echo "gcp-credentials.json" >> .gitignore
echo "kaggle.json" >> .gitignore
echo ".env" >> .gitignore

# Use environment variables
export GOOGLE_APPLICATION_CREDENTIALS=/path/to/key.json
export KAGGLE_CONFIG_DIR=$HOME/.kaggle
```

### Service Account Permissions

**Minimum Required**:
- `roles/dataproc.worker`
- `roles/storage.objectAdmin`
- `roles/logging.logWriter`

**Create Custom Role**:
```bash
gcloud iam roles create sentimentMLOps \
    --project=your-project-id \
    --title="Sentiment MLOps" \
    --permissions=dataproc.jobs.create,dataproc.jobs.get,storage.objects.get,storage.objects.create
```

### Network Security

```bash
# Restrict MLflow access
gcloud compute firewall-rules create mlflow-restricted \
    --direction=INGRESS \
    --priority=1000 \
    --network=default \
    --action=ALLOW \
    --rules=tcp:5000 \
    --source-ranges=YOUR_IP_RANGE
```

---

## 🚀 Production Deployment Checklist

### Before Going Live

- [ ] Training pipeline completed successfully
- [ ] Model registered in MLflow with good metrics (>75% accuracy)
- [ ] Dataproc cluster sized appropriately (3-5 workers)
- [ ] GCS buckets created and accessible
- [ ] Test deployment verified in test mode
- [ ] Monitoring dashboard configured
- [ ] Alert notifications set up (email/Slack)
- [ ] Backup/rollback plan documented
- [ ] Team trained on serving management DAG
- [ ] Security audit completed
- [ ] Cost estimation and budget alerts configured
- [ ] Documentation reviewed and updated
- [ ] Load testing completed
- [ ] Disaster recovery plan in place

---

## 📈 Key Technologies

| Component | Technology | Purpose |
|-----------|-----------|---------|
| Orchestration | Apache Airflow | Workflow management, scheduling, monitoring |
| Compute | GCP Dataproc | Managed Spark cluster for distributed training |
| Processing | Apache Spark | Distributed data processing and ML training |
| ML Framework | Spark MLlib | Machine learning algorithms and pipelines |
| Experiment Tracking | MLflow | Model registry, experiment tracking, versioning |
| Streaming | Spark Structured Streaming | Real-time prediction serving |
| Storage | GCS (Google Cloud Storage) | Data lake, model artifacts, predictions |
| Messaging | Apache Kafka | Real-time data ingestion/output |
| Containerization | Docker | Airflow service deployment |

---

## 🤝 Contributing

1. Fork the repository
2. Create a feature branch (`git checkout -b feature/amazing-feature`)
3. Commit your changes (`git commit -m 'Add amazing feature'`)
4. Push to the branch (`git push origin feature/amazing-feature`)
5. Open a Pull Request

---

## 📝 License

This project is licensed under the MIT License.

---

## 🙏 Acknowledgments

- **Sentiment140 Dataset**: [Kaggle Dataset](https://www.kaggle.com/datasets/kazanova/sentiment140)
- **Apache Spark**: [spark.apache.org](https://spark.apache.org/)
- **MLflow**: [mlflow.org](https://mlflow.org/)
- **Apache Airflow**: [airflow.apache.org](https://airflow.apache.org/)

---

## 📧 Support

For questions or issues:
- Open an issue on GitHub
- Check the documentation in `/sentiment-project/airflow/dags/SERVING_DAGS_README.md`
- Review logs in Airflow UI or Dataproc Console

---

**Last Updated**: December 25, 2025  
**Maintained By**: MLOps Team
