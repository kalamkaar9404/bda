"""
Kafka Producer — Real-Time Twitter Sentiment Analysis
Reads twitter_validation.csv row by row and publishes each tweet
to the Kafka topic 'twitter', simulating a real-time stream.
"""

import csv
import os
import sys
import time
from kafka import KafkaProducer

# ── Configuration ────────────────────────────────────────────────────────────
KAFKA_BROKER = "localhost:9092"
TOPIC = "twitter"
CSV_FILE = os.path.join(os.path.dirname(__file__), "twitter_validation.csv")
DELAY_SECONDS = 0.5   # delay between messages (simulates real-time stream)

# ── Producer setup ───────────────────────────────────────────────────────────
try:
    producer = KafkaProducer(
        bootstrap_servers=KAFKA_BROKER,
        value_serializer=lambda v: v.encode("utf-8"),
    )
except Exception as exc:
    print(f"[ERROR] Cannot reach Kafka at {KAFKA_BROKER}: {exc}")
    print("        Start the stack first:")
    print("        docker compose -f zk-single-kafka-single.yml up -d")
    sys.exit(1)

print(f"[Producer] Connected to Kafka at {KAFKA_BROKER}")
print(f"[Producer] Reading from: {CSV_FILE}")
print(f"[Producer] Publishing to topic: '{TOPIC}'")
print("-" * 60)

sent = 0

try:
    with open(CSV_FILE, "r", encoding="utf-8", errors="replace") as f:
        reader = csv.reader(f)
        for row in reader:
            if len(row) < 4:
                continue  # skip malformed rows

            tweet_id, entity, sentiment, tweet_text = row[0], row[1], row[2], row[3]

            # Skip header rows
            if tweet_id.strip().lower() in ("tweetid", "tweet_id", "id"):
                continue

            message = f"{tweet_id},{entity},{sentiment},{tweet_text}"
            producer.send(TOPIC, value=message)
            sent += 1
            print(f"[{sent}] Sent → {message[:80]}...")
            time.sleep(DELAY_SECONDS)

    producer.flush()
    print(f"\n[Producer] Done. Total messages sent: {sent}")

except FileNotFoundError:
    print(f"[ERROR] CSV file not found: {CSV_FILE}")
    print("Make sure twitter_validation.csv is in the Kafka-PySpark/ folder.")
except KeyboardInterrupt:
    print(f"\n[Producer] Interrupted. Messages sent so far: {sent}")
finally:
    producer.close()
