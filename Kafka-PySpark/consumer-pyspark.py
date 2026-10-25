"""
PySpark Consumer — Real-Time Twitter Sentiment Analysis
Reads from Kafka topic 'twitter' using kafka-python, classifies each
tweet with the pre-trained PySpark MLlib pipeline, and writes to MongoDB.

This version uses kafka-python for consuming (no Spark Streaming overhead)
and a single persistent SparkSession + PipelineModel for inference.
"""

import os
import re
import sys

# ── Spark environment (JDK 17, matching worker Python, winutils) ───────────
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from spark_env import apply_local_fs_fix, configure_spark_env

configure_spark_env()

from pyspark.sql import SparkSession
from pyspark.sql import Row
from pyspark.ml import PipelineModel
from kafka import KafkaConsumer
from pymongo import MongoClient

# ── Configuration ──────────────────────────────────────────────────────────────
KAFKA_BROKER       = "localhost:9092"
TOPIC              = "twitter"
MODEL_PATH         = os.path.abspath(os.path.join(
    os.path.dirname(os.path.abspath(__file__)),
    "..", "Django-Dashboard", "logistic_regression_model.pkl"
))
MONGO_URI          = "mongodb://localhost:27017/"
MONGO_DB           = "bigdata_project"
MONGO_COLLECTION   = "tweets"
BATCH_SIZE         = 20   # Process tweets in batches of this size

# Sentiment label mapping (must match training order in train_model.py)
CLASS_LABELS = {0: "Negative", 1: "Positive", 2: "Neutral", 3: "Irrelevant"}

# ── Text preprocessing ─────────────────────────────────────────────────────────
def clean_text(text):
    text = re.sub(r"https?://\S+|www\.\S+|\.com\S*", "", text)
    text = re.sub(r"(@|#)\w+", "", text)
    text = text.lower()
    text = re.sub(r"[^a-zA-Z\s]", "", text)
    text = re.sub(r"\s+", " ", text).strip()
    return text

# ── SparkSession ───────────────────────────────────────────────────────────────
print("[1/4] Starting SparkSession...")
_builder = (
    SparkSession.builder
    .appName("TwitterSentimentConsumer")
    .master("local[2]")
    .config("spark.driver.memory", "1g")
    .config("spark.sql.shuffle.partitions", "2")
)
spark = apply_local_fs_fix(_builder).getOrCreate()
spark.sparkContext.setLogLevel("ERROR")
print(f"     Spark {spark.version} started.")

# ── Load ML Model ──────────────────────────────────────────────────────────────
print("[2/4] Loading ML model...")
if not os.path.isdir(os.path.join(MODEL_PATH, "stages")):
    print(f"[FAIL] No trained model at: {MODEL_PATH}")
    print("       The saved pipeline is missing its 'stages/' directory.")
    print("       Train it first:  python train_model.py")
    spark.stop()
    sys.exit(1)
model = PipelineModel.load(MODEL_PATH)
print(f"     Model loaded from: {MODEL_PATH}")

# ── MongoDB ────────────────────────────────────────────────────────────────────
print("[3/4] Connecting to MongoDB...")
mongo_client = MongoClient(MONGO_URI, serverSelectionTimeoutMS=5000)
try:
    mongo_client.admin.command("ping")
except Exception as exc:
    print(f"[FAIL] Cannot reach MongoDB at {MONGO_URI}: {exc}")
    print("       Start it first, e.g.:  docker compose -f zk-single-kafka-single.yml up -d")
    spark.stop()
    sys.exit(1)
mongo_col = mongo_client[MONGO_DB][MONGO_COLLECTION]
print(f"     Connected -> {MONGO_DB}.{MONGO_COLLECTION}")

# ── Kafka Consumer ─────────────────────────────────────────────────────────────
print("[4/4] Connecting to Kafka...")
try:
    consumer = KafkaConsumer(
        TOPIC,
        bootstrap_servers=KAFKA_BROKER,
        auto_offset_reset="earliest",
        enable_auto_commit=True,
        group_id="pyspark-sentiment-group",
        value_deserializer=lambda m: m.decode("utf-8", errors="replace"),
    )
except Exception as exc:
    print(f"[FAIL] Cannot reach Kafka at {KAFKA_BROKER}: {exc}")
    print("       Start it first:  docker compose -f zk-single-kafka-single.yml up -d")
    spark.stop()
    sys.exit(1)
print(f"     Listening on topic '{TOPIC}' at {KAFKA_BROKER}")
print("-" * 60)
print("Streaming tweets... Press Ctrl+C to stop.\n")

# ── Classify a batch of texts using Spark ─────────────────────────────────────
def classify_batch(raw_texts):
    rows = [Row(Text=clean_text(t)) for t in raw_texts if clean_text(t)]
    if not rows:
        return []
    df = spark.createDataFrame(rows)
    preds = model.transform(df)
    results = preds.select("Text", "prediction").collect()
    return [
        {"tweet": r["Text"], "prediction": CLASS_LABELS.get(int(r["prediction"]), "Neutral")}
        for r in results
    ]

# ── Main loop ──────────────────────────────────────────────────────────────────
batch_texts = []
total_processed = 0

try:
    for msg in consumer:
        raw = msg.value
        # Parse: tweet_id, entity, sentiment, tweet_text (comma-separated, 4 fields)
        parts = raw.split(",", 3)
        tweet_text = parts[3] if len(parts) >= 4 else raw

        batch_texts.append(tweet_text)

        if len(batch_texts) >= BATCH_SIZE:
            docs = classify_batch(batch_texts)
            if docs:
                mongo_col.insert_many(docs)
                total_processed += len(docs)
                print(f"[Batch] Classified & stored {len(docs)} tweets | Total: {total_processed}")
            batch_texts = []

except KeyboardInterrupt:
    # Flush remaining
    if batch_texts:
        docs = classify_batch(batch_texts)
        if docs:
            mongo_col.insert_many(docs)
            total_processed += len(docs)
    print(f"\nStopped. Total tweets processed: {total_processed}")
finally:
    consumer.close()
    spark.stop()
