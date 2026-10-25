"""
On-demand sentiment classification for text typed by a user on /classify/.

Spark is started lazily on the first request rather than at import time, so the
Django server still boots (and the dashboard still works) when the model has not
been trained yet.
"""

import os
import re
import sys

# ── Spark environment (JDK 17, matching worker Python, winutils) ──────────────
# spark_env.py lives at the repository root, two levels above this file.
_REPO_ROOT = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
if _REPO_ROOT not in sys.path:
    sys.path.insert(0, _REPO_ROOT)
from spark_env import apply_local_fs_fix, configure_spark_env

configure_spark_env()

from pyspark.ml import PipelineModel
from pyspark.sql import SparkSession

# Absolute path, so the model is found no matter which directory Django is
# started from.
MODEL_PATH = os.path.join(
    os.path.dirname(os.path.dirname(os.path.abspath(__file__))),
    "logistic_regression_model.pkl",
)

CLASS_INDEX_MAPPING = {0: "Negative", 1: "Positive", 2: "Neutral", 3: "Irrelevant"}

# Populated on first use by _get_pipeline().
_spark = None
_pipeline = None


class ModelNotTrained(RuntimeError):
    """Raised when logistic_regression_model.pkl is missing or incomplete."""


def _get_pipeline():
    """Start Spark and load the pipeline once, reusing both across requests."""
    global _spark, _pipeline

    if _pipeline is not None:
        return _spark, _pipeline

    if not os.path.isdir(os.path.join(MODEL_PATH, "stages")):
        raise ModelNotTrained(
            f"No trained model at {MODEL_PATH} (its 'stages/' directory is missing). "
            "Train it first with: python train_model.py"
        )

    builder = (
        SparkSession.builder
        .appName("classify tweets")
        .master("local[2]")
        .config("spark.driver.memory", "1g")
        .config("spark.sql.shuffle.partitions", "2")
    )
    _spark = apply_local_fs_fix(builder).getOrCreate()
    _spark.sparkContext.setLogLevel("ERROR")
    _pipeline = PipelineModel.load(MODEL_PATH)
    return _spark, _pipeline


def clean_text(text):
    """Strip links, mentions, hashtags and punctuation; lowercase and collapse spaces."""
    if text is None:
        return ""

    # Remove links starting with https://, http://, www., or containing .com
    text = re.sub(r"https?://\S+|www\.\S+|S+\.com\S+|youtu\.be/\S+", "", text)

    # Remove words starting with # or @
    text = re.sub(r"(@|#)\w+", "", text)

    text = text.lower()

    # Remove non-alphabetic characters
    text = re.sub(r"[^a-zA-Z\s]", "", text)

    # Remove extra whitespaces
    return re.sub(r"\s+", " ", text).strip()


def classify_text(text: str) -> str:
    """Return the predicted sentiment label for a single piece of text."""
    spark, pipeline = _get_pipeline()

    preprocessed_tweet = clean_text(text)
    data = spark.createDataFrame([(preprocessed_tweet,)], ["Text"])

    # Select the prediction column by name — the old code indexed column 6 of the
    # row, which silently breaks whenever the pipeline's output columns change.
    prediction = pipeline.transform(data).select("prediction").collect()[0]["prediction"]
    label = CLASS_INDEX_MAPPING[int(prediction)]

    print("-> Tweet                      : ", text)
    print("-> preprocessed_tweet         : ", preprocessed_tweet)
    print("-> Predicted Sentiment index  : ", prediction)
    print("-> Predicted Sentiment class  : ", label)
    print("/" * 50)

    return label
