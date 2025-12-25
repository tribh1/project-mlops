#!/usr/bin/env python3
"""
Train script that logs run, params, metrics and registers Spark models with MLflow.
Trains multiple classifiers and logs them separately.

Usage example (Dataproc):
gcloud dataproc jobs submit pyspark gs://BUCKET/code/sentiment_train_mlflow.py \
  --cluster=CLUSTER --region=REGION \
  --properties="spark.executor.memory=8g,spark.executor.cores=4" \
  -- \
  --input gs://BUCKET/raw/sentiment140.csv \
  --mlflow-uri https://mlflow.example.com \
  --mlflow-experiment twitter-experiment \
  --registered-model-name twitter-sentiment-model
"""

import argparse
import re
import os
from datetime import datetime

from pyspark.sql import SparkSession, functions as F
from pyspark.sql.types import IntegerType

from pyspark.ml.feature import Tokenizer, StopWordsRemover, HashingTF, IDF
from pyspark.ml.classification import (
    LogisticRegression,
    RandomForestClassifier,
    LinearSVC,
)
from pyspark.ml import Pipeline
from pyspark.ml.evaluation import BinaryClassificationEvaluator, MulticlassClassificationEvaluator
from pyspark.ml.tuning import ParamGridBuilder, CrossValidator
from pyspark.mllib.evaluation import MulticlassMetrics
import numpy as np

# MLflow
USE_MLFLOW = True
try:
    import mlflow
    import mlflow.spark
    import matplotlib
    matplotlib.use('Agg')  # Non-interactive backend for server environments
    import matplotlib.pyplot as plt
    import seaborn as sns
    PLOT_AVAILABLE = True
except Exception as e:
    USE_MLFLOW = False
    PLOT_AVAILABLE = False
    print(f"Warning: MLflow or plotting libraries not available: {e}")

def parse_args():
    p = argparse.ArgumentParser()
    p.add_argument("--input", required=True, help="Input CSV path (gs://... or local).")
    p.add_argument("--output", required=True, help="Output base path for artifacts (gs://...)")
    p.add_argument("--mlflow-uri", required=False, default=None, help="MLflow tracking URI (e.g., https://mlflow.example.com)")
    p.add_argument("--mlflow-experiment", default="default", help="MLflow experiment name")
    p.add_argument("--registered-model-name", default="twitter-sentiment-model", help="Model name prefix to register in MLflow Model Registry")
    p.add_argument("--num-features", type=int, default=2**15, help="HashingTF numFeatures (reduced from 2^16)")  # Giảm từ 65,536 xuống 32,768
    p.add_argument("--train-ratio", type=float, default=0.8)
    p.add_argument("--val-ratio", type=float, default=0.1, help="Validation ratio (from training set)")
    p.add_argument("--max-rows", type=int, default=0, help="If >0 limit rows for quick debug")
    p.add_argument("--enable-cv", action="store_true", help="Enable cross-validation for hyperparameter tuning")
    p.add_argument("--cv-folds", type=int, default=2, help="Number of folds for cross-validation (reduced from 3)")  # Giảm từ 3 xuống 2
    p.add_argument("--enable-caching", action="store_true", help="Enable Spark DataFrame caching")
    p.add_argument("--parallelism", type=int, default=0, help="Set Spark parallelism (0 for auto)")
    p.add_argument("--sample-fraction", type=float, default=1.0, help="Fraction of data to use for training")
    return p.parse_args()

def create_spark(args):
    spark_builder = SparkSession.builder.appName("sentiment-train-mlflow-multi")
    
    # Set Spark configurations for performance
    if args.parallelism > 0:
        spark_builder = spark_builder.config("spark.default.parallelism", args.parallelism)
    
    # Optimize shuffle partitions
    spark_builder = spark_builder.config("spark.sql.shuffle.partitions", "200")  # Giảm từ mặc định 200
    
    # Disable adaptive query execution for more predictable performance
    spark_builder = spark_builder.config("spark.sql.adaptive.enabled", "false")
    
    return spark_builder.getOrCreate()

def read_and_prep(spark, input_path, max_rows=0, sample_fraction=1.0):
    # Read Sentiment140 CSV with no header
    # Dataset has 6 columns:
    # 0: polarity (0 = negative, 4 = positive)
    # 1: id
    # 2: date
    # 3: query
    # 4: user
    # 5: text
    
    # Optimize CSV reading
    df = spark.read.csv(
        input_path, 
        header=False,
        inferSchema=True  # Let Spark infer schema for faster reading
    )
    
    # Rename columns
    df = df.withColumnRenamed("_c0", "label") \
           .withColumnRenamed("_c5", "text")
    
    df = df.select("label", "text")
    
    # Cast label to integer and convert 4 → 1 (positive), 0 stays 0 (negative)
    df = df.withColumn("label", F.col("label").cast(IntegerType()))
    df = df.withColumn("label", (F.col("label") == 4).cast(IntegerType()))
    
    # Apply sampling if requested
    if sample_fraction < 1.0:
        df = df.sample(fraction=sample_fraction, seed=42)
    
    if max_rows > 0:
        df = df.limit(max_rows)
    
    return df

def create_feature_pipeline(num_features):
    """Create the common feature engineering pipeline for all models"""
    tokenizer = Tokenizer(inputCol="text", outputCol="tokens")
    remover = StopWordsRemover(inputCol="tokens", outputCol="filtered")
    hashing = HashingTF(inputCol="filtered", outputCol="rawFeatures", numFeatures=num_features)
    idf = IDF(inputCol="rawFeatures", outputCol="features", minDocFreq=5)  # Add minDocFreq to reduce features
    return Pipeline(stages=[tokenizer, remover, hashing, idf])

def get_model_configurations(args):
    """Return configurations for all models to train - optimized for speed"""
    return [
        {
            "name": "LogisticRegression",
            "model": LogisticRegression(
                featuresCol="features", 
                labelCol="label", 
                maxIter=20,  # Giảm từ 30 xuống 20
                regParam=0.1,  # Default value
                elasticNetParam=0.0
            ),
            "param_grid": None if not args.enable_cv else ParamGridBuilder()
                .addGrid(LogisticRegression.regParam, [0.01, 0.1])  # Giảm từ 2 xuống 2 giá trị
                .addGrid(LogisticRegression.elasticNetParam, [0.0, 0.5])  # Giảm số lượng
                .build(),
            "enable_cv": args.enable_cv
        },
        {
            "name": "RandomForest",
            "model": RandomForestClassifier(
                featuresCol="features", 
                labelCol="label", 
                seed=42,
                numTrees=10,  # Giảm từ 10 xuống 10 (giữ nguyên)
                maxDepth=5,
                maxBins=32,  # Giảm để tăng tốc
                subsamplingRate=0.8  # Giảm tỷ lệ sampling để tăng tốc
            ),
            "param_grid": None if not args.enable_cv else ParamGridBuilder()
                .addGrid(RandomForestClassifier.numTrees, [5, 10])  # Giảm
                .addGrid(RandomForestClassifier.maxDepth, [3, 5])
                .build(),
            "enable_cv": args.enable_cv
        },
        {
            "name": "LinearSVC",
            "model": LinearSVC(
                featuresCol="features", 
                labelCol="label", 
                maxIter=30,  # Giảm từ 50 xuống 30
                regParam=0.1
            ),
            "param_grid": None if not args.enable_cv else ParamGridBuilder()
                .addGrid(LinearSVC.regParam, [0.01, 0.1])  # 2 giá trị
                .addGrid(LinearSVC.maxIter, [20, 30])
                .build(),
            "enable_cv": args.enable_cv and False  # Disable CV for LinearSVC vì nó chậm
        }
    ]

def evaluate_model(predictions, model_name):
    """Calculate various metrics for model evaluation.
    Since the dataset is balanced (equal positive/negative samples),
    accuracy is the primary evaluation metric.
    """
    metrics = {}
    
    # Primary metric: Accuracy (for balanced dataset)
    evaluator_accuracy = MulticlassClassificationEvaluator(
        labelCol="label",
        predictionCol="prediction",
        metricName="accuracy"
    )
    metrics["accuracy"] = float(evaluator_accuracy.evaluate(predictions))
    
    # Binary classification metrics (secondary)
    evaluator_auc = BinaryClassificationEvaluator(
        labelCol="label", 
        rawPredictionCol="rawPrediction", 
        metricName="areaUnderROC"
    )
    metrics["auc"] = float(evaluator_auc.evaluate(predictions))
    
    evaluator_pr = BinaryClassificationEvaluator(
        labelCol="label", 
        rawPredictionCol="rawPrediction", 
        metricName="areaUnderPR"
    )
    metrics["pr_auc"] = float(evaluator_pr.evaluate(predictions))
    
    # Multiclass metrics using MLlib
    prediction_and_labels = predictions.select("prediction", "label").rdd.map(
        lambda row: (float(row.prediction), float(row.label))
    )
    multiclass_metrics = MulticlassMetrics(prediction_and_labels)
    
    metrics["weighted_precision"] = float(multiclass_metrics.weightedPrecision)
    metrics["weighted_recall"] = float(multiclass_metrics.weightedRecall)
    metrics["weighted_f1"] = float(multiclass_metrics.weightedFMeasure())
    
    # Confusion matrix for balanced dataset analysis
    confusion_matrix = multiclass_metrics.confusionMatrix().toArray()
    metrics["confusion_matrix"] = confusion_matrix
    
    # Class-wise metrics (0=negative, 1=positive)
    labels = [0.0, 1.0]
    for label in labels:
        try:
            metrics[f"precision_{int(label)}"] = float(multiclass_metrics.precision(label))
            metrics[f"recall_{int(label)}"] = float(multiclass_metrics.recall(label))
            metrics[f"f1_{int(label)}"] = float(multiclass_metrics.fMeasure(label))
        except:
            metrics[f"precision_{int(label)}"] = 0.0
            metrics[f"recall_{int(label)}"] = 0.0
            metrics[f"f1_{int(label)}"] = 0.0
    
    return metrics

def plot_confusion_matrix(confusion_matrix, model_name, save_path=None):
    """Plot confusion matrix for binary classification.
    Args:
        confusion_matrix: 2x2 numpy array
        model_name: name of the model
        save_path: optional path to save the plot
    Returns:
        Path to saved plot or None
    """
    if not PLOT_AVAILABLE:
        return None
    
    try:
        # Create figure
        plt.figure(figsize=(8, 6))
        
        # Plot heatmap
        sns.heatmap(
            confusion_matrix,
            annot=True,
            fmt='g',
            cmap='Blues',
            xticklabels=['Negative (0)', 'Positive (1)'],
            yticklabels=['Negative (0)', 'Positive (1)'],
            cbar_kws={'label': 'Count'}
        )
        
        plt.title(f'Confusion Matrix - {model_name}\n(Balanced Dataset)', fontsize=14, fontweight='bold')
        plt.ylabel('Actual Label', fontsize=12)
        plt.xlabel('Predicted Label', fontsize=12)
        
        # Add performance summary
        tn, fp, fn, tp = confusion_matrix.ravel()
        total = tn + fp + fn + tp
        accuracy = (tp + tn) / total
        
        plt.text(
            0.5, -0.15, 
            f'Accuracy: {accuracy:.4f} | True Neg: {int(tn)} | False Pos: {int(fp)} | False Neg: {int(fn)} | True Pos: {int(tp)}',
            ha='center',
            transform=plt.gca().transAxes,
            fontsize=10
        )
        
        plt.tight_layout()
        
        # Save plot
        if save_path:
            plt.savefig(save_path, dpi=100, bbox_inches='tight')
            plt.close()
            return save_path
        else:
            # Save to temporary file
            import tempfile
            temp_file = tempfile.NamedTemporaryFile(delete=False, suffix='.png')
            plt.savefig(temp_file.name, dpi=100, bbox_inches='tight')
            plt.close()
            return temp_file.name
    except Exception as e:
        print(f"Warning: Could not plot confusion matrix: {e}")
        return None


def train_single_model(train_df, val_df, feature_pipeline, model_config, cv_folds):
    """Train a single model with optional cross-validation"""
    
    # Create full pipeline
    full_pipeline = Pipeline(stages=feature_pipeline.getStages() + [model_config["model"]])
    
    if model_config["enable_cv"] and model_config["param_grid"]:
        # Use cross-validation with accuracy as metric (for balanced dataset)
        evaluator = MulticlassClassificationEvaluator(
            labelCol="label",
            predictionCol="prediction",
            metricName="accuracy"
        )
        
        cv = CrossValidator(
            estimator=full_pipeline,
            estimatorParamMaps=model_config["param_grid"],
            evaluator=evaluator,
            numFolds=cv_folds,
            seed=42,
            parallelism=2  # Giảm parallelism cho CV
        )
        
        cv_model = cv.fit(train_df)
        best_model = cv_model.bestModel
        best_params = cv_model.bestModel.stages[-1].extractParamMap()
        
        return best_model, best_params, cv_model
    else:
        # Train without cross-validation
        model = full_pipeline.fit(train_df)
        best_params = model.stages[-1].extractParamMap()
        return model, best_params, None

def cleanup_cache(train, val, test):
    """Unpersist DataFrames to free memory"""
    if train.is_cached:
        train.unpersist()
    if val.is_cached:
        val.unpersist()
    if test.is_cached:
        test.unpersist()

def main():
    args = parse_args()
    spark = create_spark(args)
    
    # Setup MLflow
    if USE_MLFLOW and args.mlflow_uri:
        mlflow.set_tracking_uri(args.mlflow_uri)
        mlflow.set_experiment(args.mlflow_experiment)
    
    # Read and prepare data
    df = read_and_prep(spark, args.input, max_rows=args.max_rows, sample_fraction=args.sample_fraction)
    df = df.withColumn("text", F.trim(F.col("text")))
    
    # Calculate dataset statistics
    total = df.count()
    pos = df.filter(F.col("label") == 1).count()
    neg = df.filter(F.col("label") == 0).count()
    
    # Split data
    train_val, test = df.randomSplit(
        [args.train_ratio + args.val_ratio, 1 - args.train_ratio - args.val_ratio], 
        seed=42
    )
    
    # Further split train_val into train and validation
    if args.val_ratio > 0:
        train_ratio_adjusted = args.train_ratio / (args.train_ratio + args.val_ratio)
        train, val = train_val.randomSplit([train_ratio_adjusted, 1 - train_ratio_adjusted], seed=42)
    else:
        train = train_val
        val = train_val  # Use train as val if no validation split
    
    # Cache DataFrames if enabled
    if args.enable_caching:
        train.cache()
        val.cache()
        test.cache()
        train.count()  # Trigger caching
        val.count()
        test.count()
    
    print(f"Dataset sizes - Total: {total}, Train: {train.count()}, "
          f"Val: {val.count()}, Test: {test.count()}")
    print(f"Positive samples: {pos}, Negative samples: {neg}")
    
    # Create feature pipeline
    feature_pipeline = create_feature_pipeline(args.num_features)
    
    # Get all model configurations
    model_configs = get_model_configurations(args)
    
    best_overall_auc = 0.0
    best_overall_accuracy = 0.0
    best_overall_model = None
    best_overall_model_name = ""
    
    if USE_MLFLOW and args.mlflow_uri:
        # Create a parent run for the entire experiment
        with mlflow.start_run(run_name="sentiment_multi_model_experiment") as parent_run:
            parent_run_id = parent_run.info.run_id
            
            # Log common parameters
            mlflow.log_param("num_features", args.num_features)
            mlflow.log_param("train_ratio", args.train_ratio)
            mlflow.log_param("val_ratio", args.val_ratio)
            mlflow.log_param("total_rows", total)
            mlflow.log_param("enable_cv", args.enable_cv)
            mlflow.log_param("sample_fraction", args.sample_fraction)
            mlflow.log_param("enable_caching", args.enable_caching)
            mlflow.log_metric("pos_count", pos)
            mlflow.log_metric("neg_count", neg)
            
            for model_config in model_configs:
                model_name = model_config["name"]
                print(f"\n{'='*60}")
                print(f"Training {model_name}...")
                print(f"{'='*60}")
                
                # Create a child run for each model
                with mlflow.start_run(run_name=model_name, nested=True) as child_run:
                    start_time = datetime.utcnow()
                    
                    # Train model
                    trained_model, best_params, cv_model = train_single_model(
                        train, val, feature_pipeline, model_config, 
                        args.cv_folds
                    )
                    
                    end_time = datetime.utcnow()
                    training_time = (end_time - start_time).total_seconds()
                    
                    # Make predictions
                    predictions = trained_model.transform(test)
                    
                    # Evaluate
                    metrics = evaluate_model(predictions, model_name)
                    
                    # Plot and log confusion matrix
                    confusion_matrix = metrics.pop("confusion_matrix")
                    cm_plot_path = plot_confusion_matrix(confusion_matrix, model_name)
                    if cm_plot_path:
                        try:
                            mlflow.log_artifact(cm_plot_path, "confusion_matrices")
                            # Clean up temp file
                            import os as _os
                            _os.unlink(cm_plot_path)
                        except Exception as e:
                            print(f"Warning: Could not log confusion matrix plot: {e}")
                    
                    # Log parameters
                    mlflow.log_param("model_type", model_name)
                    mlflow.log_param("num_features", args.num_features)
                    
                    # Log best hyperparameters
                    for param, value in best_params.items():
                        mlflow.log_param(f"best_{param.name}", value)
                    
                    # Log metrics
                    for metric_name, metric_value in metrics.items():
                        mlflow.log_metric(metric_name, metric_value)
                    
                    # Log training time
                    mlflow.log_metric("training_time_seconds", training_time)
                    
                    # Log model
                    artifact_path = f"spark-model-{model_name.lower()}"
                    registered_name = f"{args.registered_model_name}-{model_name.lower()}"
                    
                    try:
                        mlflow.spark.log_model(
                            spark_model=trained_model,
                            artifact_path=artifact_path,
                            registered_model_name=registered_name
                        )
                    except Exception as e:
                        print(f"Warning: Could not log model to MLflow: {e}")
                    
                    print(f"{model_name} - Accuracy: {metrics['accuracy']:.4f}, AUC: {metrics['auc']:.4f}, F1: {metrics['weighted_f1']:.4f}")
                    print(f"Training time: {training_time:.1f}s")
                    
                    # Track best model by accuracy (primary metric for balanced dataset)
                    if metrics["accuracy"] > best_overall_accuracy:
                        best_overall_accuracy = metrics["accuracy"]
                        best_overall_auc = metrics["auc"]
                        best_overall_model = trained_model
                        best_overall_model_name = model_name
                    
                    # Log cross-validation metrics if applicable
                    if cv_model and args.enable_cv:
                        cv_metrics = cv_model.avgMetrics
                        for i, cv_metric in enumerate(cv_metrics):
                            mlflow.log_metric(f"cv_fold_avg_metric_{i}", float(cv_metric))
                    
                    # Clear predictions from memory
                    try:
                        predictions.unpersist()
                    except:
                        pass
            
            # After all models are trained, log the best model info
            mlflow.log_metric("best_overall_accuracy", best_overall_accuracy)
            mlflow.log_metric("best_overall_auc", best_overall_auc)
            mlflow.log_param("best_model", best_overall_model_name)
            
            # Save the best model separately
            if best_overall_model:
                try:
                    mlflow.spark.log_model(
                        spark_model=best_overall_model,
                        artifact_path="best-spark-model",
                        registered_model_name=f"{args.registered_model_name}-best"
                    )
                except Exception as e:
                    print(f"Warning: Could not log best model to MLflow: {e}")
                
                # Save best model to output path
                model_path = os.path.join(
                    args.output.rstrip("/"), 
                    "models", 
                    f"best-model-{best_overall_model_name}-{datetime.utcnow().strftime('%Y%m%d%H%M%S')}"
                )
                try:
                    best_overall_model.write().overwrite().save(model_path)
                    print(f"\nBest model ({best_overall_model_name}) saved to: {model_path}")
                except Exception as e:
                    print(f"Warning: Could not save best model to {model_path}: {e}")
            
            print(f"\n{'='*60}")
            print(f"Training complete! Best model: {best_overall_model_name}")
            print(f"Best Accuracy: {best_overall_accuracy:.4f} (AUC: {best_overall_auc:.4f})")
            print(f"Note: Using Accuracy as primary metric for balanced dataset")
            print(f"{'='*60}")
    
    else:
        # Fallback without MLflow
        print("MLflow not available or MLflow URI not provided. Training without tracking...")
        
        for model_config in model_configs:
            model_name = model_config["name"]
            print(f"\nTraining {model_name}...")
            
            start_time = datetime.utcnow()
            trained_model, best_params, _ = train_single_model(
                train, val, feature_pipeline, model_config, 
                args.cv_folds
            )
            end_time = datetime.utcnow()
            training_time = (end_time - start_time).total_seconds()
            
            predictions = trained_model.transform(test)
            metrics = evaluate_model(predictions, model_name)
            
            # Extract and remove confusion matrix from metrics for display
            confusion_matrix = metrics.pop("confusion_matrix", None)
            
            print(f"{model_name} Results:")
            print(f"  Accuracy: {metrics['accuracy']:.4f} (primary metric)")
            print(f"  AUC: {metrics['auc']:.4f}")
            print(f"  F1 Score: {metrics['weighted_f1']:.4f}")
            print(f"  Training time: {training_time:.1f}s")
            
            # Display confusion matrix
            if confusion_matrix is not None:
                tn, fp, fn, tp = confusion_matrix.ravel()
                print(f"  Confusion Matrix: TN={int(tn)}, FP={int(fp)}, FN={int(fn)}, TP={int(tp)}")
            
            # Save model locally
            model_path = os.path.join(
                args.output.rstrip("/"), 
                "models", 
                f"{model_name.lower()}-{datetime.utcnow().strftime('%Y%m%d%H%M%S')}"
            )
            try:
                trained_model.write().overwrite().save(model_path)
                print(f"  Model saved to: {model_path}")
            except Exception as e:
                print(f"  Warning: Could not save model: {e}")
            
            # Clear predictions
            try:
                predictions.unpersist()
            except:
                pass
            
            # Track best model by accuracy
            if metrics["accuracy"] > best_overall_accuracy:
                best_overall_accuracy = metrics["accuracy"]
                best_overall_auc = metrics["auc"]
                best_overall_model = trained_model
                best_overall_model_name = model_name
        
        print(f"\nBest model: {best_overall_model_name} (Accuracy: {best_overall_accuracy:.4f}, AUC: {best_overall_auc:.4f})")
    
    # Clean up cached DataFrames
    if args.enable_caching:
        try:
            cleanup_cache(train, val, test)
        except:
            pass
    
    spark.stop()

if __name__ == "__main__":
    main()