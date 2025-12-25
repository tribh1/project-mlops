# Sentiment Analysis ML Pipeline Flow Documentation

## Overview
This document describes the complete end-to-end flow of the sentiment analysis ML pipeline, from triggering the Airflow DAG to serving predictions with the best trained model.

**Pipeline Architecture:**
```
Airflow DAG → Dataproc Cluster → Spark Training Job → MLflow Registry → Model Serving
```

---

## 1. Trigger DAG in Airflow

**Entry Point:** [airflow/dags/sentiment_training_pipeline.py](airflow/dags/sentiment_training_pipeline.py)

**DAG Name:** `sentiment_ml_training_pipeline`

**Schedule:** Weekly (`@weekly`), or can be triggered manually

### DAG Configuration Variables:
```python
# MLflow Configuration
MLFLOW_TRACKING_URI = "http://localhost:5000"  # or http://<MLFLOW_VM_IP>:5000
MLFLOW_EXPERIMENT_NAME = "sentiment-analysis-pipeline"

# GCP Dataproc Configuration
GCP_PROJECT = "sentiment-analysis-140"
GCP_REGION = "us-central1"
DATAPROC_CLUSTER = "sentiment-cluster"
GCS_BUCKET = "sentiment-bucket"
MLFLOW_VM_IP = "34.66.73.187"
```

### Initial Tasks (Validation):
1. **`start`**: Entry point (DummyOperator)
2. **`validate_data`**: Validates input CSV exists and checks row count
   - Path: `/home/felipe/sentiment-project/data_raw/sentiment140/training.1600000.processed.noemoticon.csv`
   - Pushes metadata (row count) to XCom
3. **`check_mlflow_connection`**: Verifies MLflow tracking server is accessible
   - Creates experiment if it doesn't exist
   - Confirms connection to MLflow at `http://{MLFLOW_VM_IP}:5000`
4. **`prepare_environment`**: Creates necessary directories for output

---

## 2. Submit Job to Dataproc

**Task Group:** `model_training`

**Two Parallel Training Jobs:**

### Job 1: Full Dataset Training (`train_models_full_dataset`)
Trains models on 100% of data without cross-validation for faster training.

**DataprocSubmitPySparkJobOperator Configuration:**
```python
main = 'gs://{GCS_BUCKET}-code/sentiment_multimodel.py'
cluster_name = DATAPROC_CLUSTER
region = GCP_REGION
project_id = GCP_PROJECT

arguments = [
    '--input', 'gs://{GCS_BUCKET}-raw/training.1600000.processed.noemoticon.csv',
    '--output', 'gs://{GCS_BUCKET}-artifacts',
    '--mlflow-uri', 'http://{MLFLOW_VM_IP}:5000',
    '--mlflow-experiment', 'sentiment-analysis-pipeline',
    '--registered-model-name', 'twitter-sentiment-model',
    '--num-features', '32768',
    '--train-ratio', '0.8',
    '--val-ratio', '0.1',
    '--enable-caching',
    '--sample-fraction', '1.0',
]

dataproc_properties = {
    'spark.driver.memory': '8g',
    'spark.executor.memory': '16g',
    'spark.executor.cores': '4',
    'spark.sql.shuffle.partitions': '200',
}
```

### Job 2: Cross-Validation Training (`train_models_with_cv`)
Trains models with hyperparameter tuning on 30% sample for faster experimentation.

**Additional Arguments:**
```python
'--enable-cv',
'--cv-folds', '2',
'--sample-fraction', '0.3',  # 30% of data
'--num-features', '16384',    # Reduced feature count
'--mlflow-experiment', 'sentiment-analysis-pipeline-cv',
```

**Test Mode:**
If `TEST_MODE = True`, jobs are simulated with BashOperator instead of actual Dataproc submission (for local testing).

---

## 3. Run Training Job on Dataproc

**Script:** [code/sentiment_multimodel.py](code/sentiment_multimodel.py)

### Training Flow:

#### 3.1 Data Preprocessing
```python
# Read Sentiment140 CSV (6 columns, no header)
# Columns: polarity, id, date, query, user, text
df = spark.read.csv(input_path, header=False, inferSchema=True)

# Select and transform
df = df.withColumnRenamed("_c0", "label").withColumnRenamed("_c5", "text")
df = df.withColumn("label", (F.col("label") == 4).cast(IntegerType()))  # 4→1, 0→0

# Split: 80% train, 10% val, 10% test
train_val, test = df.randomSplit([0.9, 0.1], seed=42)
train, val = train_val.randomSplit([0.89, 0.11], seed=42)
```

#### 3.2 Feature Engineering Pipeline
```python
stages = [
    Tokenizer(inputCol="text", outputCol="tokens"),
    StopWordsRemover(inputCol="tokens", outputCol="filtered"),
    HashingTF(inputCol="filtered", outputCol="rawFeatures", numFeatures=32768),
    IDF(inputCol="rawFeatures", outputCol="features", minDocFreq=5)
]
feature_pipeline = Pipeline(stages=stages)
```

#### 3.3 Model Training
**Three Models Trained in Parallel:**

1. **Logistic Regression**
   - `maxIter=20`, `regParam=0.1`
   - CV Grid: `regParam=[0.01, 0.1]`, `elasticNetParam=[0.0, 0.5]`

2. **Random Forest Classifier**
   - `numTrees=10`, `maxDepth=5`, `maxBins=32`
   - CV Grid: `numTrees=[5, 10]`, `maxDepth=[3, 5]`

3. **Linear SVC**
   - `maxIter=30`, `regParam=0.1`
   - CV Grid: `regParam=[0.01, 0.1]`, `maxIter=[20, 30]`

**Training Process per Model:**
```python
for model_config in model_configs:
    with mlflow.start_run(run_name=model_name, nested=True):
        # 1. Train model (with or without CV)
        trained_model = train_single_model(train, val, feature_pipeline, model_config, cv_folds)
        
        # 2. Make predictions on test set
        predictions = trained_model.transform(test)
        
        # 3. Evaluate metrics
        metrics = evaluate_model(predictions, model_name)
        
        # 4. Log to MLflow (next section)
```

---

## 4. Register Model Trained to MLflow

**MLflow Integration in Training Script:**

### 4.1 Create Parent Run
```python
with mlflow.start_run(run_name="sentiment_multi_model_experiment") as parent_run:
    # Log common parameters
    mlflow.log_param("num_features", num_features)
    mlflow.log_param("train_ratio", train_ratio)
    mlflow.log_param("total_rows", total_rows)
    mlflow.log_metric("pos_count", pos_count)
    mlflow.log_metric("neg_count", neg_count)
```

### 4.2 Create Child Runs for Each Model
```python
for model_config in model_configs:
    with mlflow.start_run(run_name=model_name, nested=True):
        # Log model-specific parameters
        mlflow.log_param("model_type", model_name)
        mlflow.log_param("num_features", num_features)
        
        # Log best hyperparameters from CV
        for param, value in best_params.items():
            mlflow.log_param(f"best_{param.name}", value)
        
        # Log evaluation metrics
        mlflow.log_metric("accuracy", accuracy)
        mlflow.log_metric("auc", auc)
        mlflow.log_metric("weighted_f1", f1_score)
        mlflow.log_metric("weighted_precision", precision)
        mlflow.log_metric("weighted_recall", recall)
        mlflow.log_metric("training_time_seconds", training_time)
        
        # Log confusion matrix plot
        mlflow.log_artifact(confusion_matrix_plot, "confusion_matrices")
        
        # Register model to MLflow Model Registry
        registered_name = f"twitter-sentiment-model-{model_name.lower()}"
        mlflow.spark.log_model(
            spark_model=trained_model,
            artifact_path=f"spark-model-{model_name.lower()}",
            registered_model_name=registered_name
        )
```

### 4.3 Register Best Model
```python
# After all models trained, register best model separately
mlflow.spark.log_model(
    spark_model=best_overall_model,
    artifact_path="best-spark-model",
    registered_model_name="twitter-sentiment-model-best"
)

# Also save to GCS
model_path = f"gs://{GCS_BUCKET}-artifacts/models/best-model-{best_model_name}-{timestamp}"
best_overall_model.write().overwrite().save(model_path)
```

**Registered Models in MLflow:**
- `twitter-sentiment-model-logisticregression`
- `twitter-sentiment-model-randomforest`
- `twitter-sentiment-model-linearsvc`
- `twitter-sentiment-model-best` (highest accuracy)

**MLflow Tracking UI:** `http://{MLFLOW_VM_IP}:5000`

---

## 5. Get Best Model

**Back in Airflow DAG:**

**Task:** `get_best_model_info`

```python
def get_best_model_info(**context):
    mlflow.set_tracking_uri(MLFLOW_TRACKING_URI)
    client = MlflowClient()
    
    experiment = mlflow.get_experiment_by_name(MLFLOW_EXPERIMENT_NAME)
    
    # Search runs sorted by accuracy (primary metric)
    runs = mlflow.search_runs(
        experiment_ids=[experiment.experiment_id],
        order_by=["metrics.accuracy DESC"],
        max_results=1
    )
    
    best_run = runs.iloc[0]
    best_model_info = {
        'run_id': best_run.run_id,
        'accuracy': best_run['metrics.accuracy'],
        'auc': best_run['metrics.auc'],
        'model_type': best_run['params.model_type'],
        'run_name': best_run['tags.mlflow.runName']
    }
    
    # Push to XCom for downstream tasks
    context['task_instance'].xcom_push(key='best_model_info', value=best_model_info)
    
    return best_model_info
```

**Output Example:**
```json
{
  "run_id": "abc123...",
  "accuracy": 0.7856,
  "auc": 0.8543,
  "model_type": "LogisticRegression",
  "run_name": "LogisticRegression"
}
```

### Post-Training Tasks:
1. **`generate_training_report`**: Creates summary report in `models/training_report.txt`
2. **`send_completion_notification`**: Logs/sends notification with training results
3. **`end`**: Completion marker

---

## 6. Serving Model

**Script:** [code/streaming_serving_mlflow_hdfs_kafka_optimize.py](code/streaming_serving_mlflow_hdfs_kafka_optimize.py)

### Serving Architecture:
```
Input Source (File/Kafka) → Spark Structured Streaming → Load Model from MLflow 
→ Transform (Predict) → Output (HDFS/GCS + Optional Kafka)
```

### 6.1 Launch Serving Job

**Key Parameters:**
- Input source: File (HDFS/GCS) or Kafka stream
- Model: `twitter-sentiment-model-best` from MLflow (Production stage)
- Batch interval: 30 seconds
- Model reload check: Every 300 seconds

### 6.2 Model Loading with Hot-Swap

**Model Management:**
- Model cached in driver memory for performance
- Checks MLflow Model Registry for new versions every 300 seconds
- Automatically reloads when new version detected
- Model URI format: `models:/twitter-sentiment-model-best/{version}`
- Supports both Spark MLlib and PyFunc model flavors

### 6.3 Streaming Prediction Flow

**Input Sources:**
- **File Source:** Watches HDFS/GCS directory for new CSV files
- **Kafka Source:** Consumes real-time messages from Kafka topic

**Processing Steps (per batch):**
1. Check if model reload needed (every 300 seconds)
2. Transform batch using loaded model: `model.transform(batch_df)`
3. Add metadata: prediction timestamp, model version, date
4. Write to output sinks:
   - Parquet files (partitioned by date) to GCS
   - Optional Kafka output topic for real-time consumers
5. Log batch metrics and processing statistics

### 6.4 Output Structure

**Predictions Output:**
- Location: `gs://sentiment-bucket-predictions/predictions/`
- Format: Parquet files partitioned by date
- Schema: text, label, features, prediction, probability, prediction_timestamp, model_version, date

---

## Complete Pipeline Summary

### Flow Diagram:
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
```

---

## Key Technologies

| Component | Technology | Purpose |
|-----------|-----------|---------|
| Orchestration | Apache Airflow | Workflow management, scheduling, monitoring |
| Compute | GCP Dataproc | Managed Spark cluster for distributed training |
| Processing | Apache Spark | Distributed data processing and ML training |
| ML Framework | Spark MLlib | Machine learning algorithms and pipelines |
| Experiment Tracking | MLflow | Model registry, experiment tracking, versioning |
| Streaming | Spark Structured Streaming | Real-time prediction serving |
| Storage | GCS (Google Cloud Storage) | Data lake, model artifacts, predictions |
| Messaging | Apache Kafka (optional) | Real-time data ingestion/output |

---

## Monitoring & Observability

### Airflow UI
- **URL:** `http://localhost:8080` (or Airflow server)
- **View:** DAG runs, task logs, execution times, XCom data

### MLflow UI
- **URL:** `http://34.66.73.187:5000`
- **View:** Experiments, runs, metrics comparison, registered models

### Dataproc Monitoring
- **URL:** GCP Console → Dataproc → Clusters → Jobs
- **View:** Job status, driver logs, executor logs, resource utilization

### Logs Locations
```
/home/felipe/sentiment-project/airflow/logs/
  dag_id=sentiment_ml_training_pipeline/
    run_id=manual__2025-12-22T00:00:00+00:00/
      task_id=check_mlflow_connection/
      task_id=model_training.train_models_full_dataset/
      task_id=get_best_model_info/
```

---

## Deployment Checklist

### Prerequisites:
- ✅ Dataproc cluster running: `sentiment-cluster`
- ✅ MLflow tracking server: `http://34.66.73.187:5000`
- ✅ GCS buckets created:
  - `gs://sentiment-bucket-code/`
  - `gs://sentiment-bucket-raw/`
  - `gs://sentiment-bucket-artifacts/`
  - `gs://sentiment-bucket-jars/`
- ✅ Training data uploaded: `training.1600000.processed.noemoticon.csv`
- ✅ Airflow variables configured
- ✅ GCP connection configured in Airflow

### Running the Pipeline:
```bash
# 1. Start Airflow
cd /home/felipe/sentiment-project/airflow
docker-compose up -d

# 2. Trigger DAG (via UI or CLI)
airflow dags trigger sentiment_ml_training_pipeline

# 3. Monitor progress in Airflow UI
# http://localhost:8080

# 4. Check MLflow for results
# http://34.66.73.187:5000

# 5. Deploy serving (after training completes)
gcloud dataproc jobs submit pyspark \
  gs://sentiment-bucket-code/streaming_serving_mlflow_hdfs_kafka_optimize.py \
  --cluster=sentiment-cluster \
  --region=us-central1 \
  -- \
  --source-type file \
  --input-path gs://sentiment-bucket-raw/streaming/ \
  --output-path gs://sentiment-bucket-predictions/ \
  --checkpoint gs://sentiment-bucket-checkpoint/ \
  --mlflow-uri http://34.66.73.187:5000 \
  --model-name twitter-sentiment-model-best \
  --model-stage Production \
  --model-flavor spark
```

---

## Troubleshooting

### Common Issues:

**1. MLflow Connection Failed**
- Check MLflow server status: `curl http://34.66.73.187:5000/health`
- Verify firewall rules allow port 5000
- Check MLflow server logs

**2. Dataproc Job Failed**
- View logs in GCP Console → Dataproc → Jobs → [Job ID]
- Check driver logs for Python errors
- Verify GCS paths are correct and accessible

**3. Model Not Loading in Serving**
- Confirm model exists in MLflow: `mlflow models list --model-name twitter-sentiment-model-best`
- Check model version and stage
- Verify Spark cluster can access MLflow tracking URI

**4. Out of Memory Errors**
- Reduce `--sample-fraction`
- Decrease `--num-features`
- Increase Spark executor memory in Dataproc properties

---

## Performance Metrics

**Typical Pipeline Execution Times:**
- Data validation: ~30 seconds
- Full training job: ~30-45 minutes (1.6M rows, 3 models)
- CV training job: ~15-20 minutes (30% sample, 2 folds)
- Model registration: ~5 minutes
- Best model selection: ~10 seconds

**Model Performance (Expected):**
- Accuracy: 75-80%
- AUC: 80-85%
- F1 Score: 75-80%

**Resource Utilization:**
- Driver memory: 8GB
- Executor memory: 16GB per executor
- Executor cores: 4 cores per executor
- Typical cluster: 3-5 worker nodes

---

## Future Enhancements

- [ ] Add model A/B testing in serving layer
- [ ] Implement automated model promotion (Staging → Production)
- [ ] Add model performance monitoring and drift detection
- [ ] Integrate with real-time Kafka streams for production data
- [ ] Add email/Slack notifications on training completion
- [ ] Implement model explainability (SHAP values)
- [ ] Add data quality checks and validation
- [ ] Implement rolling deployments for model updates

---

**Document Version:** 1.0  
**Last Updated:** December 22, 2025  
**Maintained By:** Data Science Team
