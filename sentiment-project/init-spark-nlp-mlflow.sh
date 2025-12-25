#!/bin/bash
set -eux

# Install pip packages on cluster nodes
# Note: adjust python/pip path per image (this uses system python)
PIP_PACKAGES="mlflow==2.6.0 pyspark==3.4.0 spark-nlp==4.4.0 boto3 google-cloud-storage"

# Install python packages
if command -v pip3 >/dev/null 2>&1; then
  PIP_BIN=pip3
elif command -v pip >/dev/null 2>&1; then
  PIP_BIN=pip
else
  echo "pip not found"
  exit 1
fi

$PIP_BIN install --upgrade $PIP_PACKAGES --no-cache-dir

# Optionally download spark-nlp jar to a central GCS path for --jars
# You may want to pick the correct scala/ spark version jar from Maven coordinates
# Example (commented placeholder):
# wget -q -P /tmp https://repo1.maven.org/maven2/com/johnsnowlabs/spark-nlp_2.12/4.4.0/spark-nlp_2.12-4.4.0.jar
# gsutil cp /tmp/spark-nlp_2.12-4.4.0.jar gs://$BUCKET/jars/

echo "Init action completed"
