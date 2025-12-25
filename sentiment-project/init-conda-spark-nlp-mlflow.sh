#!/bin/bash
set -euxo pipefail

# Init action: install required Python packages with pip3 and place Spark-NLP jar into Spark jars dir
# Logs written to /var/log/dataproc-init-sparknlp.log for easier debugging
exec > /var/log/dataproc-init-sparknlp.log 2>&1

# Helper: download with retries
download_with_retries() {
	local url="$1"
	local out="$2"
	local tries=0
	local max=5
	while [ $tries -lt $max ]; do
		if curl --retry 5 --retry-delay 5 --connect-timeout 30 --max-time 180 -fSL "$url" -o "$out"; then
			return 0
		fi
		tries=$((tries+1))
		echo "download attempt $tries failed for $url"
		sleep 4
	done
	return 1
}

# Helper: apt-get install with retries (handles transient apt/network issues)
apt_install_retry() {
	local pkgs=("$@")
	local i=0
	local max=5
	export DEBIAN_FRONTEND=noninteractive
	while [ $i -lt $max ]; do
		if apt-get update -y && apt-get install -y "${pkgs[@]}"; then
			return 0
		fi
		i=$((i+1))
		echo "apt-get attempt $i failed, retrying in 5s..."
		sleep 5
	done
	return 1
}

echo "[init] Ensure pip3 exists"
if ! command -v pip3 >/dev/null 2>&1; then
	if command -v apt-get >/dev/null 2>&1; then
		echo "Installing python3-pip via apt-get (with retries)"
		if ! apt_install_retry python3-pip; then
			echo "Failed to install python3-pip via apt-get" >&2
			exit 1
		fi
	else
		echo "pip3 not found and apt-get not available; cannot install pip3" >&2
		exit 1
	fi
fi

echo "[init] upgrading pip (with retries)"
for attempt in 1 2 3; do
	if pip3 install --upgrade pip setuptools wheel --no-cache-dir; then
		break
	fi
	echo "pip upgrade attempt $attempt failed, retrying..."
	sleep 5
	if [ $attempt -eq 3 ]; then
		echo "pip upgrade failed after $attempt attempts" >&2
		exit 1
	fi
done

echo "[init] installing python packages via pip (with retries)"
PIP_PACKAGES=(mlflow==2.6.0 pyspark==3.4.0 spark-nlp==4.4.0 boto3 google-cloud-storage)
for attempt in 1 2 3; do
	if pip3 install --no-cache-dir --upgrade "${PIP_PACKAGES[@]}"; then
		echo "pip install succeeded"
		break
	fi
	echo "pip install attempt $attempt failed"
	sleep 5
	if [ $attempt -eq 3 ]; then
		echo "pip install failed after $attempt attempts" >&2
		exit 1
	fi
done

# Install Spark-NLP jar into Spark's jars dir so JVM annotators work
SPARKNLP_VERSION="4.4.0"
SCALA_SUFFIX="2.12"
JAR_NAME="spark-nlp_${SCALA_SUFFIX}-${SPARKNLP_VERSION}.jar"
# Construct canonical Maven Central URL for the artifact
JAR_URL="https://repo1.maven.org/maven2/com/johnsnowlabs/nlp/spark-nlp_${SCALA_SUFFIX}/${SPARKNLP_VERSION}/${JAR_NAME}"
TMP_JAR="/tmp/${JAR_NAME}"
SPARK_JARS_DIR="/usr/lib/spark/jars"

echo "[init] fetching Spark-NLP jar $JAR_URL"
if ! download_with_retries "$JAR_URL" "$TMP_JAR"; then
	echo "Primary jar download failed"
	if [ -n "${SPARKNLP_GCS_URI:-}" ]; then
		echo "Attempting to copy jar from GCS: ${SPARKNLP_GCS_URI}"
		if command -v gsutil >/dev/null 2>&1; then
			if gsutil cp "${SPARKNLP_GCS_URI}" "$TMP_JAR"; then
				echo "Copied Spark-NLP jar from GCS"
			else
				echo "Failed to copy Spark-NLP jar from GCS URI: ${SPARKNLP_GCS_URI}" >&2
				exit 1
			fi
		else
			echo "gsutil not available to fetch jar from GCS" >&2
			exit 1
		fi
	else
		echo "No SPARKNLP_GCS_URI set; cannot obtain spark-nlp jar" >&2
		exit 1
	fi
fi

mkdir -p "$SPARK_JARS_DIR"
mv "$TMP_JAR" "$SPARK_JARS_DIR/$JAR_NAME"
chmod 644 "$SPARK_JARS_DIR/$JAR_NAME"

echo "[init] Spark-NLP jar installed to $SPARK_JARS_DIR/$JAR_NAME"

echo "Initialization action completed"
exit 0
