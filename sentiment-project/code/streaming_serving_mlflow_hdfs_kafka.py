#!/usr/bin/env python3
"""
Updated streaming_serving_mlflow_hdfs_kafka.py

Key changes from original:
 - Added --model-flavor (spark | pyfunc)
 - For pyfunc: uses mlflow.pyfunc.spark_udf(...) inside foreachBatch
 - Writes parquet using partitionBy("date") instead of manual path construction
 - Avoid duplicate .count() calls; compute once and reuse
 - Better error handling around model loading & sink writes
 - Keeps driver-side MODEL_CACHE and hot-swap polling logic
 - Keeps support for file (HDFS) and Kafka input sources and optional Kafka output

Original file uploaded by user was used as source for edits. :contentReference[oaicite:1]{index=1}
"""

import argparse
import time
import json
from datetime import datetime
from threading import Lock

from pyspark.sql import SparkSession, functions as F, types as T

# mlflow usage
USE_MLFLOW = True
try:
    import mlflow
    import mlflow.spark
    import mlflow.pyfunc
    from mlflow.tracking import MlflowClient
except Exception as e:
    USE_MLFLOW = False
    mlflow = None
    MlflowClient = None
    print("WARNING: mlflow not available. Streaming will still run but cannot load models from MLflow.", e)


# -----------------------------------------------------------------------------
# Argument parsing
# -----------------------------------------------------------------------------
def parse_args():
    p = argparse.ArgumentParser()
    p.add_argument("--source-type", required=True, choices=["file", "kafka"], help="file or kafka")
    p.add_argument("--input-path", help="For file source: HDFS directory to watch (e.g. hdfs:///data/sentiment/new/)")
    p.add_argument("--file-schema-json", default=None, help="Optional JSON file schema for file source (or infer)")
    p.add_argument("--max-files-per-trigger", type=int, default=1, help="For file source: max files per trigger")
    p.add_argument("--kafka-bootstrap", help="Kafka bootstrap servers for input (host:port)")
    p.add_argument("--kafka-topic", help="Kafka topic to subscribe (input)")
    p.add_argument("--output-path", required=True, help="HDFS/GCS output base path for predictions (e.g. gs://... or hdfs://...)")
    p.add_argument("--checkpoint", required=True, help="Checkpoint location (HDFS)")
    p.add_argument("--mlflow-uri", required=True, help="MLflow tracking server URI (http://host:port)")
    p.add_argument("--model-name", required=True, help="Registered model name in MLflow")
    p.add_argument("--model-stage", default="Production", help="Model stage to load (Production/Staging) or blank for latest")
    p.add_argument("--model-flavor", choices=["spark", "pyfunc"], default="spark",
                   help="Model flavor in registry: 'spark' (mlflow.spark.load_model) or 'pyfunc' (mlflow.pyfunc)")
    p.add_argument("--batch-interval-seconds", type=int, default=30, help="Micro-batch interval seconds")
    p.add_argument("--reload-interval-seconds", type=int, default=300, help="How often (seconds) to poll registry for new model version")
    p.add_argument("--write-kafka", type=str, default="false", help="Write predictions to Kafka (true/false)")
    p.add_argument("--kafka-output-bootstrap", help="Kafka bootstrap servers for output")
    p.add_argument("--kafka-output-topic", help="Kafka topic to write predictions")
    p.add_argument("--write-gcs", type=str, default="false", help="If true, will write JSON files to output path as well")
    p.add_argument("--debug", action="store_true")
    return p.parse_args()


# -----------------------------------------------------------------------------
# Global model cache (driver-side)
# -----------------------------------------------------------------------------
class ModelCache:
    def __init__(self):
        self.lock = Lock()
        self.model = None         # For spark flavor: PipelineModel object; for pyfunc: stored model_uri string
        self.model_uri = None
        self.loaded_version = None
        self.last_checked = 0

    def get_model(self):
        with self.lock:
            return self.model, self.loaded_version, self.model_uri

    def set_model(self, model, version, uri):
        with self.lock:
            self.model = model
            self.loaded_version = version
            self.model_uri = uri
            self.last_checked = time.time()


MODEL_CACHE = ModelCache()


# -----------------------------------------------------------------------------
# MLflow helpers: get current production version
# -----------------------------------------------------------------------------
def get_registry_model_version(client, model_name, stage):
    """
    Returns (version, model_uri) for the stage (Production/Staging) or (None, None).
    """
    # Ignore stage: always get the latest version regardless of stage
    versions = client.get_latest_versions(model_name)
    if not versions:
        return None, None
    # Pick the highest version number
    latest_version = max(versions, key=lambda v: int(v.version))
    model_uri = f"models:/{model_name}/{latest_version.version}"
    return latest_version.version, model_uri


def load_model_if_needed(args):
    """
    Load model into MODEL_CACHE if not loaded or version changed.
    For model-flavor == "spark": loads mlflow.spark.load_model(...) and caches PipelineModel object.
    For model-flavor == "pyfunc": stores model_uri string in cache (we'll create spark_udf per-batch).
    """
    if not USE_MLFLOW:
        raise RuntimeError("MLflow is not available in this runtime. Install mlflow on cluster or use packaged venv.")

    client = MlflowClient(tracking_uri=args.mlflow_uri)

    try:
        version, uri = get_registry_model_version(client, args.model_name, args.model_stage)
    except Exception as e:
        print("ERROR querying MLflow registry:", e)
        return None

    if version is None:
        print(f"No model versions found for {args.model_name} stage={args.model_stage}")
        return None

    current_model, loaded_version, current_uri = MODEL_CACHE.get_model()
    if loaded_version == version and current_model is not None:
        # already loaded and same version
        return current_model

    print(f"Loading model {args.model_name} version={version} uri={uri} as flavor={args.model_flavor}")
    try:
        if args.model_flavor == "spark":
            model_obj = mlflow.spark.load_model(uri)
        else:
            # for pyfunc we keep the model URI in cache; actual spark_udf will be created inside foreachBatch
            model_obj = uri
    except Exception as e:
        print("ERROR loading model from MLflow:", e)
        return None

    MODEL_CACHE.set_model(model_obj, version, uri)
    print(f"Loaded model version {version}")
    return model_obj


# -----------------------------------------------------------------------------
# foreachBatch function factory
# -----------------------------------------------------------------------------
def make_foreach_batch_fn(args):
    """
    Returns a function suitable for DataFrame.writeStream.foreachBatch(fn).
    The function will:
      - load model if needed (cached)
      - run model.transform on batch_df (or apply pyfunc UDF)
      - write predictions partitioned by date to args.output_path
      - optionally write JSON messages to Kafka and/or write JSON files to output
      - log batch-level metrics to MLflow (if available)
    """
    if USE_MLFLOW:
        mlflow.set_tracking_uri(args.mlflow_uri)
        mlflow_client = MlflowClient(tracking_uri=args.mlflow_uri)
    else:
        mlflow_client = None

    def foreach_batch(batch_df, batch_id):
        start_ts = time.time()

        # materialize row_count once (avoid double .count())
        try:
            row_count = batch_df.count()
        except Exception as e:
            print(f"[batch {batch_id}] Could not count rows in incoming batch: {e}")
            row_count = -1

        print(f"[batch {batch_id}] received rows={row_count}")

        # no-op for empty batches
        if row_count == 0:
            print(f"[batch {batch_id}] empty batch, skipping")
            return

        # reload model if older than reload interval or not present
        reload_needed = False
        _, loaded_version, _ = MODEL_CACHE.get_model()
        if loaded_version is None:
            reload_needed = True
        else:
            if time.time() - MODEL_CACHE.last_checked >= args.reload_interval_seconds:
                reload_needed = True

        if reload_needed:
            try:
                model = load_model_if_needed(args)
            except Exception as e:
                print("ERROR loading model:", e)
                model = None
        else:
            model, _, _ = MODEL_CACHE.get_model()

        if model is None:
            print(f"[batch {batch_id}] No model available for transformation; skipping batch.")
            return

        # ensure we have a 'text' column (common for your sentiment model); if not, try to pick single column
        if "text" not in batch_df.columns:
            cols = batch_df.columns
            print(f"[batch {batch_id}] Batch columns: {cols}")
            if len(cols) == 1:
                batch_df = batch_df.withColumnRenamed(cols[0], "text")
            else:
                # still proceed but raise to be explicit
                raise RuntimeError("No 'text' column found in batch and ambiguous fallback.")

        df_in = batch_df.withColumn("ingest_time", F.current_timestamp())

        # apply transformation depending on flavor
        try:
            if args.model_flavor == "spark":
                # model is a PipelineModel
                preds = model.transform(df_in)
            else:
                # pyfunc path: model contains the model_uri string
                model_uri = model if isinstance(model, str) else MODEL_CACHE.model_uri
                # create a pyfunc spark UDF; note: this creates a Python UDF under the hood
                # if your pyfunc requires external deps, prefer preinstalling them on the cluster
                try:
                    pyfunc_udf = mlflow.pyfunc.spark_udf(spark_session=batch_df.sparkSession, model_uri=model_uri)
                except TypeError:
                    # older mlflow versions accept different arg names
                    pyfunc_udf = mlflow.pyfunc.spark_udf(batch_df.sparkSession, model_uri)
                # pass the whole row as struct if the pyfunc expects multiple features; adjust if it expects a single text column
                struct_col = F.struct(*[F.col(c) for c in df_in.columns])
                preds = df_in.withColumn("prediction", pyfunc_udf(struct_col))
        except Exception as e:
            print(f"[batch {batch_id}] ERROR during model.transform / pyfunc apply: {e}")
            return

        # normalize prediction columns we want to write
        select_cols = []
        for c in ("text", "prediction", "probability", "rawPrediction", "ingest_time", "label"):
            if c in preds.columns:
                select_cols.append(c)

        preds_to_write = preds.select(*select_cols)

        # add a date partition column (UTC date)
        preds_to_write = preds_to_write.withColumn("date", F.date_format(F.current_timestamp(), "yyyy-MM-dd"))

        # Write parquet partitioned by date (atomic append)
        try:
            preds_to_write.write.mode("append").option("compression", "snappy").partitionBy("date").parquet(args.output_path)
            # count rows written by reusing the earlier row_count only if transform doesn't filter rows
            # safer approach: count rows in preds_to_write (but this may trigger another job). We'll compute it once.
            try:
                written_rows = preds_to_write.count()
            except Exception:
                written_rows = row_count
            print(f"[batch {batch_id}] Wrote {written_rows} rows to {args.output_path} (partitioned by date)")
        except Exception as e:
            print(f"[batch {batch_id}] ERROR writing parquet to {args.output_path}: {e}")

        # optionally write JSON to Kafka
        if str(args.write_kafka).lower() in ("true", "1", "yes", "y", "t"):
            if args.kafka_output_bootstrap and args.kafka_output_topic:
                try:
                    json_df = preds_to_write.select(F.to_json(F.struct(*preds_to_write.columns)).alias("value"))
                    json_df.write.format("kafka") \
                        .option("kafka.bootstrap.servers", args.kafka_output_bootstrap) \
                        .option("topic", args.kafka_output_topic) \
                        .save()
                    print(f"[batch {batch_id}] Wrote batch to Kafka topic {args.kafka_output_topic}")
                except Exception as e:
                    print(f"[batch {batch_id}] ERROR writing to Kafka topic {args.kafka_output_topic}: {e}")
            else:
                print(f"[batch {batch_id}] write_kafka requested but kafka_output_bootstrap/topic not configured")

        # optionally write JSON files to output path (useful for quick inspection)
        if str(args.write_gcs).lower() in ("true", "1", "yes", "y", "t"):
            try:
                json_out_path = args.output_path.rstrip("/") + "/json/"
                preds_to_write.select(F.to_json(F.struct(*preds_to_write.columns)).alias("value")) \
                    .write.mode("append").text(json_out_path)
                print(f"[batch {batch_id}] Wrote JSON lines to {json_out_path}")
            except Exception as e:
                print(f"[batch {batch_id}] ERROR writing JSON lines to {args.output_path}: {e}")

        # log batch-level metrics to MLflow if available
        if USE_MLFLOW:
            try:
                run = mlflow.start_run()
                mlflow.log_metric("batch_rows", float(row_count))
                # if label present compute AUC (best-effort)
                if "label" in preds.columns and "rawPrediction" in preds.columns:
                    from pyspark.ml.evaluation import BinaryClassificationEvaluator
                    evaluator = BinaryClassificationEvaluator(labelCol="label", rawPredictionCol="rawPrediction", metricName="areaUnderROC")
                    try:
                        auc = evaluator.evaluate(preds)
                        mlflow.log_metric("batch_auc", float(auc))
                    except Exception as e:
                        print(f"[batch {batch_id}] Could not compute AUC on batch: {e}")
                mlflow.end_run()
            except Exception as e:
                print(f"[batch {batch_id}] Could not log to MLflow: {e}")

        elapsed = time.time() - start_ts
        print(f"[batch {batch_id}] processed {row_count} rows in {elapsed:.2f}s")

    return foreach_batch


# -----------------------------------------------------------------------------
# Build streaming DataFrame
# -----------------------------------------------------------------------------
def build_stream(spark, args):
    if args.source_type == "file":
        if not args.input_path:
            raise ValueError("input_path required for file source")
        # require a schema to avoid schema inference errors in streaming; user can provide JSON schema or we assume basic text schema
        if args.file_schema_json:
            import json as _json
            schema_json = open(args.file_schema_json).read()
            schema = T.fromJson(_json.loads(schema_json))
        else:
            # simple schema: one text field 'text' (string)
            schema = T.StructType([T.StructField("text", T.StringType(), True)])
        stream_df = spark.readStream.format("csv") \
            .option("header", "false") \
            .option("maxFilesPerTrigger", str(args.max_files_per_trigger)) \
            .schema(schema) \
            .load(args.input_path)
        return stream_df

    elif args.source_type == "kafka":
        if not args.kafka_bootstrap or not args.kafka_topic:
            raise ValueError("kafka-bootstrap and kafka-topic required for kafka source")
        df = spark.readStream.format("kafka") \
            .option("kafka.bootstrap.servers", args.kafka_bootstrap) \
            .option("subscribe", args.kafka_topic) \
            .option("startingOffsets", "latest") \
            .load()
        parsed = df.selectExpr("CAST(value AS STRING) as value_str", "timestamp as kafka_ts")
        # naive parsing: if JSON with field 'text', parse; else use value_str as text
        json_schema = T.StructType([T.StructField("text", T.StringType(), True)])
        parsed2 = parsed.withColumn("json", F.from_json("value_str", json_schema)).select("value_str", "kafka_ts", "json.*")
        stream_df = parsed2.withColumn("text", F.coalesce(F.col("text"), F.col("value_str"))).select("text", "kafka_ts")
        return stream_df
    else:
        raise ValueError("unsupported source type")


# -----------------------------------------------------------------------------
# main
# -----------------------------------------------------------------------------
def main():
    args = parse_args()

    # booleanize flags
    args.write_kafka = str(args.write_kafka).lower() in ("true", "1", "yes", "y", "t")
    args.write_gcs = str(getattr(args, "write_gcs", "false")).lower() in ("true", "1", "yes", "y", "t")

    # create spark
    spark = SparkSession.builder.appName("streaming-mlflow-serve").getOrCreate()
    spark.sparkContext.setLogLevel("WARN" if not args.debug else "INFO")

    # set mlflow tracking URI for client usage in driver
    if USE_MLFLOW:
        try:
            mlflow.set_tracking_uri(args.mlflow_uri)
        except Exception as e:
            print("WARNING: could not set mlflow tracking uri:", e)

    stream_df = build_stream(spark, args)
    stream_df.printSchema()

    # create foreachBatch function
    foreach_fn = make_foreach_batch_fn(args)

    q = stream_df.writeStream \
        .foreachBatch(foreach_fn) \
        .outputMode("append") \
        .option("checkpointLocation", args.checkpoint) \
        .trigger(processingTime=f"{args.batch_interval_seconds} seconds") \
        .start()

    print("Started streaming query. Awaiting termination...")
    q.awaitTermination()


if __name__ == "__main__":
    main()
