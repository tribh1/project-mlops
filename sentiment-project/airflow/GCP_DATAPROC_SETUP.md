# GCP Dataproc Configuration for Airflow

## Prerequisites

1. **GCP Dataproc Cluster**: Running cluster with Spark installed
2. **Service Account**: With Dataproc permissions
3. **Credentials JSON**: Service account key file

## Setup Steps

### 1. Add GCP Service Account Credentials

```bash
# Copy your GCP service account key to airflow directory
cp /path/to/your-service-account-key.json ./gcp-credentials.json

# Update docker-compose.yml to mount credentials
# Add under volumes in airflow-common:
#   - ./gcp-credentials.json:/opt/airflow/gcp-credentials.json:ro
```

### 2. Update docker-compose.yml

Add to `airflow-common-env`:
```yaml
GOOGLE_APPLICATION_CREDENTIALS: /opt/airflow/gcp-credentials.json
GCP_PROJECT_ID: your-project-id
GCP_REGION: your-region
DATAPROC_CLUSTER_NAME: your-cluster-name
```

### 3. Create Dataproc Connection in Airflow

**Option A: Using DataprocSubmitJobOperator (Recommended)**

In your DAG, replace SparkSubmitOperator with:
```python
from airflow.providers.google.cloud.operators.dataproc import DataprocSubmitJobOperator

train_job = DataprocSubmitJobOperator(
    task_id="submit_pyspark_job",
    job={
        "reference": {"project_id": "{{ var.value.GCP_PROJECT_ID }}"},
        "placement": {"cluster_name": "{{ var.value.DATAPROC_CLUSTER_NAME }}"},
        "pyspark_job": {
            "main_python_file_uri": "gs://your-bucket/code/sentiment_train_mlflow.py",
            "args": [
                "--input", "gs://your-bucket/data/tweets.csv",
                "--output", "gs://your-bucket/models/",
                "--mlflow-uri", "http://mlflow:5000"
            ],
            "jar_file_uris": [],
            "python_file_uris": []
        },
    },
    region="{{ var.value.GCP_REGION }}",
    project_id="{{ var.value.GCP_PROJECT_ID }}",
    gcp_conn_id="google_cloud_default",
)
```

**Option B: Using SparkSubmitOperator with Livy**

```python
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator

train_job = SparkSubmitOperator(
    task_id="spark_job",
    application="/opt/airflow/code/sentiment_train_mlflow.py",
    conn_id="spark_dataproc",
    conf={
        "spark.master": "yarn",
        "spark.submit.deployMode": "cluster",
        "spark.hadoop.fs.gs.project.id": "{{ var.value.GCP_PROJECT_ID }}",
        "spark.hadoop.google.cloud.auth.service.account.enable": "true",
        "spark.hadoop.google.cloud.auth.service.account.json.keyfile": "/opt/airflow/gcp-credentials.json"
    }
)
```

### 4. Create Google Cloud Connection

```bash
docker-compose exec airflow-webserver airflow connections add 'google_cloud_default' \
    --conn-type 'google_cloud_platform' \
    --conn-extra '{
      "keyfile_path": "/opt/airflow/gcp-credentials.json",
      "project": "YOUR_PROJECT_ID",
      "scope": "https://www.googleapis.com/auth/cloud-platform"
    }'
```

### 5. Set Airflow Variables

```bash
docker-compose exec airflow-webserver airflow variables set GCP_PROJECT_ID "your-project-id"
docker-compose exec airflow-webserver airflow variables set GCP_REGION "us-central1"
docker-compose exec airflow-webserver airflow variables set DATAPROC_CLUSTER_NAME "your-cluster-name"
```

### 6. Upload Code to GCS

```bash
# Upload training scripts to GCS bucket
gsutil cp ../code/sentiment_train_mlflow.py gs://your-bucket/code/
gsutil cp ../code/*.py gs://your-bucket/code/

# Upload data if not already there
gsutil cp ../data_raw/sentiment140/training.1600000.processed.noemoticon.csv \
    gs://your-bucket/data/
```

## Architecture Changes

```
┌──────────────────────┐
│  Airflow Container   │
│  (Orchestrator)      │
└──────────┬───────────┘
           │ Submit Job via Dataproc API
           ▼
┌─────────────────────────────────────┐
│     GCP Dataproc Cluster            │
│                                     │
│  ┌─────────────────────────────┐   │
│  │   Spark Master (YARN)       │   │
│  └───────────┬─────────────────┘   │
│              │                      │
│  ┌───────────▼─────────────────┐   │
│  │  Worker 1  Worker 2  Worker 3│  │
│  │  (Executors across nodes)    │  │
│  └─────────────────────────────┘   │
└─────────────────────────────────────┘
           │
           ▼
┌─────────────────────┐      ┌──────────────┐
│   Google Cloud      │      │   MLflow     │
│   Storage (GCS)     │◄─────┤   Server     │
│   - Data            │      │   (Local or  │
│   - Models          │      │    Cloud)    │
│   - Artifacts       │      └──────────────┘
└─────────────────────┘
```

## Benefits of Using Dataproc

1. **Scalability**: Auto-scale worker nodes based on load
2. **Managed Service**: No infrastructure maintenance
3. **Cost-Effective**: Pay only for running time
4. **Integration**: Native GCS, BigQuery, and Cloud Storage
5. **Performance**: High-speed networking between nodes

## Performance Tuning

### Dataproc Cluster Configuration

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

### Spark Configuration for Large Datasets

```python
conf = {
    "spark.executor.memory": "8g",
    "spark.executor.cores": "4",
    "spark.executor.instances": "10",
    "spark.driver.memory": "4g",
    "spark.sql.shuffle.partitions": "200",
    "spark.default.parallelism": "80",
    "spark.dynamicAllocation.enabled": "true",
    "spark.dynamicAllocation.minExecutors": "2",
    "spark.dynamicAllocation.maxExecutors": "20",
}
```

## Monitoring

### View Spark UI
```bash
# Get Dataproc master node SSH
gcloud dataproc clusters describe your-cluster-name --region=us-central1

# Access Spark UI via Component Gateway
# https://CLUSTER_ID.REGION.dataproc.googleusercontent.com/
```

### Check Job Status
```bash
gcloud dataproc jobs list --cluster=your-cluster-name --region=us-central1
gcloud dataproc jobs describe JOB_ID --region=us-central1
```

## Cost Optimization

1. **Use Preemptible Workers**: 80% cost reduction
   ```bash
   --num-preemptible-workers=5
   ```

2. **Auto-scaling**: Scale down when idle
   ```bash
   --enable-component-gateway \
   --autoscaling-policy=YOUR_POLICY
   ```

3. **Auto-delete**: Delete cluster after job completes
   ```bash
   --max-idle=600s  # Delete after 10 mins idle
   ```

4. **Ephemeral Clusters**: Create cluster per job, delete after
   ```python
   create_cluster = DataprocCreateClusterOperator(...)
   submit_job = DataprocSubmitJobOperator(...)
   delete_cluster = DataprocDeleteClusterOperator(...)
   
   create_cluster >> submit_job >> delete_cluster
   ```

## Troubleshooting

### Connection Issues
```bash
# Test Dataproc API access
gcloud dataproc clusters list --region=us-central1

# Verify service account permissions
gcloud projects get-iam-policy YOUR_PROJECT_ID \
    --flatten="bindings[].members" \
    --filter="bindings.members:serviceAccount:YOUR_SA@YOUR_PROJECT.iam.gserviceaccount.com"
```

### Job Failures
```bash
# View job logs
gcloud dataproc jobs describe JOB_ID --region=us-central1
gcloud logging read "resource.type=cloud_dataproc_cluster AND resource.labels.cluster_name=CLUSTER_NAME" --limit=50
```

## Migration Checklist

- [ ] Create GCP Dataproc cluster
- [ ] Generate service account with Dataproc permissions
- [ ] Download service account key JSON
- [ ] Add credentials to Airflow container
- [ ] Update docker-compose.yml with GCP env vars
- [ ] Install `apache-airflow-providers-google` in requirements.txt
- [ ] Upload code to GCS
- [ ] Upload data to GCS
- [ ] Update DAG to use DataprocSubmitJobOperator
- [ ] Create google_cloud_default connection
- [ ] Set GCP Airflow variables
- [ ] Test job submission
- [ ] Configure MLflow to save artifacts to GCS
