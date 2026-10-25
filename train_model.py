"""
Train PySpark MLlib Sentiment Analysis Model
Reads twitter_training.csv → trains pipeline → saves to logistic_regression_model.pkl
Run with: .venv\Scripts\python train_model.py
"""

import os
import sys
import shutil

# ── Spark environment (JDK 17, matching worker Python, winutils) ───────────
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
from spark_env import apply_local_fs_fix, configure_spark_env

configure_spark_env()

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, udf, when
from pyspark.sql.types import StringType, IntegerType
from pyspark.ml import Pipeline
from pyspark.ml.feature import Tokenizer, StopWordsRemover, CountVectorizer
from pyspark.ml.classification import LogisticRegression

# ── Paths ──────────────────────────────────────────────────────────────────
BASE_DIR   = os.path.dirname(os.path.abspath(__file__))
CSV_PATH   = os.path.join(BASE_DIR, "twitter_training.csv")
MODEL_OUT  = os.path.join(BASE_DIR, "Django-Dashboard", "logistic_regression_model.pkl")

print("=" * 60)
print("PySpark Twitter Sentiment Model Trainer")
print("=" * 60)
print(f"Training data : {CSV_PATH}")
print(f"Model output  : {MODEL_OUT}")
print()

# ── SparkSession ────────────────────────────────────────────────────────────
_builder = (
    SparkSession.builder
    .appName("TwitterSentimentTraining")
    .master("local[*]")
    .config("spark.driver.memory", "2g")
)
spark = apply_local_fs_fix(_builder).getOrCreate()
spark.sparkContext.setLogLevel("ERROR")
print(f"[OK] SparkSession started — Spark {spark.version}")

# ── Load CSV ────────────────────────────────────────────────────────────────
print("[..] Loading training data...")
df_raw = spark.read.csv(CSV_PATH, header=False, inferSchema=False)
df_raw = df_raw.toDF("TweetID", "Entity", "Sentiment", "Text")

# Drop nulls and clean
df = df_raw.dropna(subset=["Sentiment", "Text"])

# Keep only valid sentiment labels
valid_sentiments = ["Positive", "Negative", "Neutral", "Irrelevant"]
df = df.filter(col("Sentiment").isin(valid_sentiments))

# Map sentiment → numeric label
label_udf = udf(lambda s: {
    "Negative": 0,
    "Positive": 1,
    "Neutral":  2,
    "Irrelevant": 3,
}.get(s, 2), IntegerType())

df = df.withColumn("label", label_udf(col("Sentiment")))

total = df.count()
print(f"[OK] Loaded {total:,} training rows")
df.groupBy("Sentiment").count().orderBy("Sentiment").show()

# ── Build ML Pipeline ───────────────────────────────────────────────────────
print("[..] Building pipeline...")

tokenizer = Tokenizer(inputCol="Text", outputCol="tokens")

remover = StopWordsRemover(inputCol="tokens", outputCol="filtered_tokens")

vectorizer = CountVectorizer(
    inputCol="filtered_tokens",
    outputCol="features",
    vocabSize=10000,
    minDF=2.0
)

lr = LogisticRegression(
    featuresCol="features",
    labelCol="label",
    maxIter=20,
    regParam=0.1,
    elasticNetParam=0.0,
    family="multinomial"
)

pipeline = Pipeline(stages=[tokenizer, remover, vectorizer, lr])

# ── Train ───────────────────────────────────────────────────────────────────
print("[..] Training model (this may take 1-3 minutes)...")
model = pipeline.fit(df)
print("[OK] Training complete!")

# ── Evaluate quickly ────────────────────────────────────────────────────────
predictions = model.transform(df)
correct = predictions.filter(col("prediction") == col("label")).count()
accuracy = correct / total * 100
print(f"[OK] Training accuracy: {accuracy:.2f}%")

# ── Save Model ──────────────────────────────────────────────────────────────
print(f"[..] Saving model to: {MODEL_OUT}")
if os.path.exists(MODEL_OUT):
    shutil.rmtree(MODEL_OUT)
    print("[  ] Removed old (corrupt) model")

try:
    model.save(MODEL_OUT)
except Exception as exc:
    if "HADOOP_HOME" in str(exc) or "winutils" in str(exc):
        print()
        print("=" * 70)
        print("[FAIL] Spark could not WRITE the model on this Windows machine.")
        print()
        print("Training itself succeeded - only saving failed. Hadoop needs")
        print("winutils.exe to set file permissions when writing from Windows.")
        print()
        print("Build the bundled winutils-free filesystem shim, then re-run:")
        print()
        print("    python tools/build_barelocalfs.py")
        print("    python train_model.py")
        print()
        print("Alternatively, train inside a Linux container:")
        print()
        print("    docker compose -f docker-compose.train.yml run --rm trainer")
        print("=" * 70)
        spark.stop()
        sys.exit(1)
    raise
print(f"[OK] Model saved successfully!")

# Verify structure
print("\n[OK] Model directory structure:")
for root, dirs, files in os.walk(MODEL_OUT):
    level = root.replace(MODEL_OUT, "").count(os.sep)
    indent = "  " * level
    print(f"{indent}{os.path.basename(root)}/")
    for f in files:
        print(f"{indent}  {f}")

spark.stop()
print("\nDone! You can now run the consumer.")
