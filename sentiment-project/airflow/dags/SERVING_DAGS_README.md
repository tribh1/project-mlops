# Sentiment Model Serving DAGs

This directory contains two separate DAGs for managing model serving, completely independent from the training pipeline.

## DAGs Overview

### 1. `sentiment_model_serving` - Deploy Model Serving
**Purpose:** Deploy the best trained model from MLflow to Dataproc for real-time predictions.

**Trigger:** Manual only (not scheduled)

**What it does:**
1. ✅ Validates MLflow connection
2. 🔍 Gets the best model from MLflow Registry
3. 🚀 Deploys streaming serving job to Dataproc
4. 📊 Monitors deployment status
5. 📧 Sends deployment notification

**Use this when:** You want to start serving predictions with a trained model.

### 2. `sentiment_serving_management` - Manage Serving Jobs
**Purpose:** Monitor, check status, and manage active serving jobs.

**Trigger:** Manual only

**What it does:**
1. 📋 Lists all active serving jobs on Dataproc
2. 📈 Shows job performance metrics
3. 📁 Checks prediction output files
4. 📝 Creates monitoring summary report

**Use this when:** You want to check if serving is working correctly or troubleshoot issues.

---

## Quick Start Guide

### Prerequisites
Before deploying serving, ensure:
- ✅ Training pipeline has completed successfully
- ✅ At least one model exists in MLflow Registry
- ✅ Dataproc cluster is running
- ✅ GCS buckets are created

### Step 1: Configure Airflow Variables

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

### Step 2: Deploy Serving (Test Mode)

1. Open Airflow UI: `http://localhost:8080`
2. Find DAG: `sentiment_model_serving`
3. Click the play button ▶️ to trigger
4. Monitor execution in Graph or Grid view

**Test mode will:**
- ✅ Validate configuration
- ✅ Check model availability
- ❌ NOT actually deploy to Dataproc
- ✅ Show what would be deployed

### Step 3: Deploy Serving (Production Mode)

When ready for production:

```bash
# Switch to production mode
airflow variables set serving_deploy_mode "production"

# Ensure GCS paths exist
gsutil mb gs://sentiment-bucket-predictions || true
gsutil mb gs://sentiment-bucket-checkpoint || true
gsutil mb gs://sentiment-bucket-streaming-input || true

# Trigger the DAG again
```

Then trigger `sentiment_model_serving` DAG in Airflow UI.

### Step 4: Monitor Serving

Trigger `sentiment_serving_management` DAG to:
- Check if serving job is running
- View prediction output
- Get performance metrics
- Generate monitoring report

---

## Serving Modes

### File Source (Default)
Watches a GCS directory for new CSV files and processes them in real-time.

**Configuration:**
```python
serving_source_type = "file"
# Input: gs://sentiment-bucket-streaming-input/
# Processes files as they arrive
```

**Test it:**
```bash
# Upload a test file
gsutil cp data_raw/sentiment140/sample_100.csv \
  gs://sentiment-bucket-streaming-input/batch_$(date +%Y%m%d_%H%M%S).csv

# Check predictions
gsutil ls -lh gs://sentiment-bucket-predictions/predictions/
```

### Kafka Source
Reads messages from Kafka topic in real-time.

**Configuration:**
```python
serving_source_type = "kafka"
kafka_bootstrap_servers = "your-kafka:9092"
kafka_input_topic = "sentiment-input"
kafka_output_topic = "sentiment-predictions"
```

**Message format:**
```json
{
  "text": "I love this product!",
  "id": "12345",
  "timestamp": "2025-12-22T10:00:00Z"
}
```

---

## Architecture

```
┌─────────────────────────────────────────────────────────┐
│ Airflow DAG: sentiment_model_serving                    │
│  1. Get best model from MLflow                          │
│  2. Deploy to Dataproc cluster                          │
└────────────────┬────────────────────────────────────────┘
                 │
                 ▼
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
                 ↓
┌─────────────────────────────────────────────────────────┐
│ Predictions Output                                       │
│  gs://sentiment-bucket-predictions/predictions/         │
│    ├─ date=2025-12-22/                                  │
│    │   ├─ part-00000.parquet                            │
│    │   └─ part-00001.parquet                            │
│    └─ date=2025-12-23/                                  │
│        └─ part-00000.parquet                            │
└─────────────────────────────────────────────────────────┘
```

---

## Model Hot-Swap Feature

The serving job automatically checks for new model versions every 5 minutes (configurable).

**How it works:**
1. Serving job loads model v1 from MLflow
2. Training pipeline registers new model v2
3. After 5 minutes, serving detects v2
4. Serving reloads model v2 without stopping
5. New predictions use v2 automatically

**No downtime!** 🎉

---

## Monitoring & Troubleshooting

### Check Serving Status

**Option 1: Use Management DAG**
- Trigger `sentiment_serving_management` in Airflow
- View logs for detailed status

**Option 2: Command Line**
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
gsutil cp gs://sentiment-bucket-predictions/predictions/date=2025-12-22/part-00000.parquet .

# Read with Python
import pandas as pd
df = pd.read_parquet('part-00000.parquet')
print(df.head())
```

### Common Issues

**1. "Model not found in MLflow"**
- Solution: Run training pipeline first (`sentiment_ml_training_pipeline`)
- Verify: Check MLflow UI at http://34.66.73.187:5000

**2. "Dataproc cluster not found"**
- Solution: Ensure cluster is running
- Command: `gcloud dataproc clusters list --region=us-central1`

**3. "No predictions being written"**
- Solution: Check if input files/messages are arriving
- For file source: Upload test file to gs://sentiment-bucket-streaming-input/
- Check serving job logs in Dataproc

**4. "Out of memory error"**
- Solution: Reduce batch size or increase executor memory
- Edit DAG: Increase `spark.executor.memory` in dataproc_properties

---

## Stopping Serving

### Stop via GCP Console
1. Go to: https://console.cloud.google.com/dataproc/jobs
2. Find running streaming job
3. Click "Kill" button

### Stop via Command Line
```bash
# List jobs to get JOB_ID
gcloud dataproc jobs list --cluster=sentiment-cluster --region=us-central1

# Kill the job
gcloud dataproc jobs kill JOB_ID --region=us-central1
```

### Stop and Clean Checkpoint (Fresh Start)
```bash
# Kill job
gcloud dataproc jobs kill JOB_ID --region=us-central1

# Delete checkpoint (forces fresh start)
gsutil rm -r gs://sentiment-bucket-checkpoint/*

# Redeploy via Airflow DAG
```

---

## Performance Tuning

### Adjust Batch Interval
```bash
# Process faster (every 10 seconds)
airflow variables set serving_batch_interval "10"

# Process slower (every 60 seconds) - better for batch workloads
airflow variables set serving_batch_interval "60"
```

### Adjust Model Reload Interval
```bash
# Check for new models more frequently (every 2 minutes)
airflow variables set serving_reload_interval "120"

# Check less frequently (every 10 minutes)
airflow variables set serving_reload_interval "600"
```

### Scale Dataproc Cluster
```bash
# Add more workers for higher throughput
gcloud dataproc clusters update sentiment-cluster \
  --region=us-central1 \
  --num-workers=5

# Increase executor memory
# Edit DAG dataproc_properties:
'spark.executor.memory': '16g'
```

---

## Production Checklist

Before deploying to production:

- [ ] Training pipeline completed successfully
- [ ] Model registered in MLflow with good metrics (>75% accuracy)
- [ ] Dataproc cluster sized appropriately (3-5 workers)
- [ ] GCS buckets created and accessible
- [ ] Test deployment verified in test mode
- [ ] Monitoring dashboard configured
- [ ] Alert notifications set up (email/Slack)
- [ ] Backup/rollback plan documented
- [ ] Team trained on serving management DAG

---

## Integration with Training Pipeline

The serving DAGs are **completely separate** from the training pipeline:

| Pipeline | Purpose | Schedule | Dependencies |
|----------|---------|----------|--------------|
| `sentiment_ml_training_pipeline` | Train models | Weekly | None |
| `sentiment_model_serving` | Deploy serving | Manual | Training must complete |
| `sentiment_serving_management` | Monitor serving | Manual | Serving deployed |

**Typical workflow:**
1. Training pipeline runs weekly (or on-demand)
2. New model registered to MLflow
3. Serving job auto-detects new model (hot-swap)
4. No manual intervention needed! ✨

**Manual deployment workflow:**
1. Training pipeline completes
2. User triggers `sentiment_model_serving` DAG
3. Serving starts on Dataproc
4. Use `sentiment_serving_management` to monitor

---

## Advanced Configuration

### Custom Model Selection
Instead of "best" model, deploy a specific model:

```bash
airflow variables set serving_model_name "twitter-sentiment-model-logisticregression"
```

### Multi-Model Serving
Deploy multiple models in parallel by duplicating the DAG with different names:
- `sentiment_serving_model_a` → LogisticRegression
- `sentiment_serving_model_b` → RandomForest

### A/B Testing
Deploy two versions and route traffic:
1. Deploy model v1 to cluster A
2. Deploy model v2 to cluster B
3. Split input traffic between clusters
4. Compare prediction outputs

---

## Support & Logs

**Airflow Logs:**
```
/opt/airflow/logs/
  dag_id=sentiment_model_serving/
  dag_id=sentiment_serving_management/
```

**Dataproc Logs:**
- GCP Console → Dataproc → Jobs → [Job ID] → Logs
- Driver logs contain model loading and prediction info

**Prediction Output:**
```
gs://sentiment-bucket-predictions/predictions/
  partitioned by date
```

---

## Next Steps

1. ✅ Run training pipeline to create a model
2. ✅ Configure serving variables in Airflow
3. ✅ Test deployment in test mode
4. ✅ Deploy to production
5. ✅ Monitor with management DAG
6. ✅ Set up alerts and notifications

**Questions?** Check the main [PIPELINE_FLOW_DOCUMENTATION.md](../../PIPELINE_FLOW_DOCUMENTATION.md)

---

**Last Updated:** December 22, 2025  
**Maintained By:** ML Ops Team
