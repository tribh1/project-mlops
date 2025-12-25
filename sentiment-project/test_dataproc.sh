#!/bin/bash
# Test script to submit a simple job to Dataproc and monitor it

set -e

# Configuration
export CLUSTER=sentiment-cluster
export REGION=us-central1
export PROJECT=sentiment-analysis-140
export BUCKET=sentiment-bucket

echo "=========================================="
echo "Dataproc Job Submission Test"
echo "=========================================="
echo "Cluster: $CLUSTER"
echo "Region: $REGION"
echo "Project: $PROJECT"
echo "=========================================="

# Upload test script to GCS
echo ""
echo "[1/4] Uploading test script to GCS..."
gsutil cp code/test_dataproc_job.py gs://$BUCKET-code/test_dataproc_job.py
echo "✓ Script uploaded"

# Check cluster status
echo ""
echo "[2/4] Checking cluster status..."
gcloud dataproc clusters describe $CLUSTER \
    --region=$REGION \
    --project=$PROJECT \
    --format="value(status.state)" || {
        echo "✗ Cluster not found or not accessible"
        exit 1
    }
echo "✓ Cluster is accessible"

# Submit job
echo ""
echo "[3/4] Submitting PySpark job to Dataproc..."
JOB_ID=$(gcloud dataproc jobs submit pyspark \
    --cluster=$CLUSTER \
    --region=$REGION \
    --project=$PROJECT \
    gs://$BUCKET-code/test_dataproc_job.py \
    --format="value(reference.jobId)")

echo "✓ Job submitted with ID: $JOB_ID"
echo ""
echo "=========================================="
echo "Monitoring Job"
echo "=========================================="

# Monitor job
echo ""
echo "[4/4] Waiting for job to complete..."
gcloud dataproc jobs wait $JOB_ID \
    --region=$REGION \
    --project=$PROJECT

# Get job details
echo ""
echo "=========================================="
echo "Job Results"
echo "=========================================="
gcloud dataproc jobs describe $JOB_ID \
    --region=$REGION \
    --project=$PROJECT \
    --format="table(
        reference.jobId,
        status.state,
        status.stateStartTime,
        placement.clusterName
    )"

echo ""
echo "✓ Job logs:"
gcloud dataproc jobs describe $JOB_ID \
    --region=$REGION \
    --project=$PROJECT \
    --format="value(driverOutputResourceUri)"

echo ""
echo "=========================================="
echo "View job in console:"
echo "https://console.cloud.google.com/dataproc/jobs/$JOB_ID?region=$REGION&project=$PROJECT"
echo "=========================================="
