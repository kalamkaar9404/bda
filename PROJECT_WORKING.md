# Real-Time Twitter Sentiment Analysis — Detailed Working Documentation

> **Project Title:** Big Data Project: Real-Time Twitter Sentiment Analysis  
> **Technologies:** Apache Kafka · Apache Spark (PySpark / MLlib) · MongoDB · Django · NLTK · Matplotlib · Chart.js  
> **University:** Abdelmalek Essaadi University — Faculty of Sciences and Technology of Tangier  
> **Master Program:** Artificial Intelligence and Data Science — Module: Big Data  

---

## Table of Contents

1. [Project Overview](#1-project-overview)
2. [High-Level Architecture](#2-high-level-architecture)
3. [Technology Stack](#3-technology-stack)
4. [Dataset Description](#4-dataset-description)
5. [Component 1 — Apache Kafka (Message Broker)](#5-component-1--apache-kafka-message-broker)
6. [Component 2 — Machine Learning Model (PySpark MLlib)](#6-component-2--machine-learning-model-pyspark-mllib)
7. [Component 3 — Spark Streaming Consumer (PySpark)](#7-component-3--spark-streaming-consumer-pyspark)
8. [Component 4 — MongoDB (Data Storage)](#8-component-4--mongodb-data-storage)
9. [Component 5 — Django Dashboard (Visualization)](#9-component-5--django-dashboard-visualization)
10. [Data Flow — End to End](#10-data-flow--end-to-end)
11. [File & Directory Structure](#11-file--directory-structure)
12. [Configuration Files](#12-configuration-files)
13. [Python Dependencies](#13-python-dependencies)
14. [How to Run the Project](#14-how-to-run-the-project)
15. [Summary](#15-summary)

---

## 1. Project Overview

This project implements a **real-time sentiment analysis pipeline** for Twitter data. It simulates live tweet ingestion using a Kafka producer reading from a CSV dataset, processes tweets through a pre-trained Spark MLlib logistic regression model, persists results in MongoDB, and displays them on an interactive Django web dashboard.

The core problem solved is:

> **Given a tweet's text, classify it as one of four sentiments: `Positive`, `Negative`, `Neutral`, or `Irrelevant` — in real time.**

---

## 2. High-Level Architecture

```
+--------------------+       +------------------+       +----------------------+
|   CSV Dataset      |       |  Apache Kafka     |       |   PySpark Streaming  |
| (twitter_valid.csv)|  -->  |  Topic: twitter   |  -->  |   + MLlib Model      |
+--------------------+       |  (Docker)         |       |   (LogisticRegress.) |
                             +------------------+       +----------+-----------+
                                                                    |
                                                                    v
                                                         +---------------------+
                                                         |     MongoDB         |
                                                         |  DB: bigdata_project|
                                                         |  Collection: tweets |
                                                         +----------+----------+
                                                                    |
                                                                    v
                                                         +---------------------+
                                                         |  Django Dashboard   |
                                                         |  (http://127.0.0.1) |
                                                         |  - Sentiment stats  |
                                                         |  - Tweet table      |
                                                         |  - Classify page    |
                                                         +---------------------+
```

**Data flows left-to-right:**
1. The **Kafka Producer** reads tweets from a CSV file and publishes them to a Kafka topic.
2. The **Spark Streaming Consumer** subscribes to that Kafka topic, preprocesses each tweet, runs it through the trained ML pipeline, and writes results to MongoDB.
3. The **Django Dashboard** reads from MongoDB and renders sentiment statistics and individual tweet predictions in a web browser.

---

## 3. Technology Stack

| Technology | Role | Version |
|---|---|---|
| **Apache Kafka** | Real-time message queue / event streaming | 7.3.0 (Confluent Docker image) |
| **Apache ZooKeeper** | Kafka cluster coordination | 7.3.0 (Confluent Docker image) |
| **PySpark (Spark MLlib)** | Stream processing + ML inference | 3.5.1 |
| **PySpark Streaming** | Micro-batch processing of Kafka events | 3.5.1 |
| **MongoDB** | NoSQL persistence for tweet sentiment results | Latest |
| **PyMongo** | Python driver for MongoDB | 3.12.1 |
| **Django** | Web framework for the dashboard | 5.0.6 |
| **NLTK** | Text preprocessing (tokenization, stopwords) | 3.8.1 |
| **Matplotlib / Seaborn** | Server-side chart generation (PNG exports) | 3.8.3 / 0.13.2 |
| **Chart.js** | Frontend interactive charts | (CDN) |
| **Pandas** | DataFrame operations in dashboard | 2.2.2 |
| **Docker / Docker Compose** | Container for Kafka + ZooKeeper | Latest |

---

## 4. Dataset Description

**Source:** [Kaggle — Twitter Entity Sentiment Analysis](https://www.kaggle.com/datasets/jp797498e/twitter-entity-sentiment-analysis)

Two CSV files are used:

| File | Purpose | Size |
|---|---|---|
| `twitter_training.csv` | Training the ML model | ~74,682 rows |
| `twitter_validation.csv` | Kafka producer input (simulates live tweets) | 998 rows |

Each row has **4 columns**:

```
Tweet ID  |  Entity   |  Sentiment  |  Tweet Content
(int)     |  (string) |  (string)   |  (string)
```

**Sentiment labels:**
- `Positive` — The tweet expresses a positive opinion about the entity.
- `Negative` — The tweet expresses a negative opinion.
- `Neutral` — No strong sentiment.
- `Irrelevant` — The tweet is unrelated to the entity.

---

## 5. Component 1 — Apache Kafka (Message Broker)

### 5.1 What is Kafka?

Apache Kafka is a distributed event streaming platform. It acts as a **message bus** between the data producer (CSV reader) and the data consumer (Spark Streaming). This decouples data ingestion from processing.

### 5.2 Docker Setup (`zk-single-kafka-single.yml`)

Kafka runs inside Docker using two containers:

**ZooKeeper (`zoo1`):**
- Image: `confluentinc/cp-zookeeper:7.3.0`
- Port: `2181`
- Manages Kafka broker metadata and leader election.

**Kafka Broker (`kafka1`):**
- Image: `confluentinc/cp-kafka:7.3.0`
- Ports exposed:
  - `9092` → External access from the host machine (producer/consumer connect here)
  - `29092` → Internal Docker network communication
  - `9999` → JMX monitoring port
- Connects to ZooKeeper at `zoo1:2181`
- Single-broker setup (replication factor = 1)
- ACL Authorizer enabled but allows all if no ACL found (development mode)

**Start command:**
```bash
docker-compose -f zk-single-kafka-single.yml up -d
```

### 5.3 Kafka Topic

A topic named **`twitter`** is created to hold tweet messages:

```bash
kafka-topics --create --topic twitter --bootstrap-server localhost:9092
```

Each **message** in the topic is a single tweet row from the CSV (tweet ID, entity, sentiment, text).

### 5.4 Kafka Producer (`producer-validation-tweets.py`)

- Located in `Kafka-PySpark/`
- Reads `twitter_validation.csv` row by row.
- Serializes each tweet into a message and **publishes** it to the `twitter` Kafka topic.
- Simulates a real-time stream by sending messages with a small delay between each one.
- Uses the `kafka-python` library (`kafka==1.3.5`).

---

## 6. Component 2 — Machine Learning Model (PySpark MLlib)

### 6.1 Model Training (Jupyter Notebook — `ML PySpark Model/`)

The ML model was trained **offline** in a Jupyter Notebook using the `twitter_training.csv` dataset. The training pipeline consists of the following stages:

#### Stage 0 — Tokenizer
- Splits raw tweet text into individual word tokens.
- PySpark component: `pyspark.ml.feature.Tokenizer`
- Input: `Text` column (cleaned tweet string)
- Output: `words` column (list of tokens)

#### Stage 1 — StopWordsRemover
- Removes common English stop words (e.g., "the", "is", "at") that do not carry sentiment.
- PySpark component: `pyspark.ml.feature.StopWordsRemover`
- Input: `words`
- Output: `filtered_words`

#### Stage 2 — CountVectorizer
- Converts the filtered word list into a **sparse numerical feature vector** (Bag of Words).
- Each position in the vector corresponds to a word in the vocabulary.
- PySpark component: `pyspark.ml.feature.CountVectorizer`
- Input: `filtered_words`
- Output: `features` (SparseVector)

#### Stage 3 — Logistic Regression
- A multi-class logistic regression classifier trained on the feature vectors.
- PySpark component: `pyspark.ml.classification.LogisticRegression`
- Input: `features`
- Output: `prediction` (0=Negative, 1=Positive, 2=Neutral, 3=Irrelevant)

### 6.2 Saved Model

The trained model is saved as a **PySpark PipelineModel** at:

```
Django-Dashboard/logistic_regression_model.pkl/
├── metadata/
└── stages/
    ├── 0_Tokenizer_65a3f58f8a72/
    ├── 1_StopWordsRemover_38204f94e3a7/
    └── 2_CountVectorizer_22a1a5907394/
```

> **Note:** Despite the `.pkl` extension (a Python convention), this is actually a PySpark model saved in Parquet/JSON format (the native PySpark format). It is loaded using `PipelineModel.load(...)`, not Python's `pickle`.

### 6.3 Class Index Mapping

```python
class_index_mapping = {
    0: "Negative",
    1: "Positive",
    2: "Neutral",
    3: "Irrelevant"
}
```

---

## 7. Component 3 — Spark Streaming Consumer (PySpark)

### 7.1 File: `Kafka-PySpark/consumer-pyspark.py`

This is the core data processing engine. It:

1. **Creates a SparkSession** — the entry point for all Spark operations.
2. **Reads from the Kafka topic `twitter`** using Spark Structured Streaming.
3. **Preprocesses** each incoming tweet message:
   - Removes URLs (`https://`, `http://`, `www.`, `.com`)
   - Removes Twitter handles (`@username`) and hashtags (`#tag`)
   - Converts to lowercase
   - Removes non-alphabetic characters
   - Strips extra whitespace
4. **Loads the saved PipelineModel** (`logistic_regression_model.pkl`).
5. **Applies the pipeline** to each micro-batch of tweets:
   - Tokenizes → Removes stopwords → Vectorizes → Predicts sentiment
6. **Maps the numeric prediction** back to its sentiment label string.
7. **Writes each result to MongoDB** (`bigdata_project.tweets` collection).

### 7.2 MongoDB Document Structure

Each processed tweet is stored as a MongoDB document:

```json
{
  "_id": ObjectId("..."),
  "tweet": "Original tweet text here",
  "prediction": "Positive"
}
```

### 7.3 Text Preprocessing (Both in Consumer and Dashboard)

Both the Kafka consumer and the Django dashboard share similar text cleaning logic:

```python
def clean_text(text):
    # Remove URLs
    text = re.sub(r'https?://\S+|www\.\S+', '', text)
    # Remove @mentions and #hashtags
    text = re.sub(r'(@|#)\w+', '', text)
    # Lowercase
    text = text.lower()
    # Remove non-alphabetic characters
    text = re.sub(r'[^a-zA-Z\s]', '', text)
    # Remove extra spaces
    text = re.sub(r'\s+', ' ', text).strip()
    return text
```

---

## 8. Component 4 — MongoDB (Data Storage)

### 8.1 Role

MongoDB is the **central data store** for all processed sentiment data. It is a NoSQL document database, making it ideal for storing variable-length tweet data without a rigid schema.

### 8.2 Connection Details

```python
client = MongoClient('mongodb://localhost:27017/')
db = client['bigdata_project']
collection = db['tweets']
```

| Setting | Value |
|---|---|
| Host | `localhost` |
| Port | `27017` |
| Database | `bigdata_project` |
| Collection | `tweets` |

### 8.3 Data Written by Spark Consumer

Each document in the `tweets` collection has at least:
- `tweet` — the cleaned or original tweet text
- `prediction` — the sentiment label string (`Positive`, `Negative`, `Neutral`, `Irrelevant`)

### 8.4 Data Read by Django

The Django dashboard queries **all documents** from the `tweets` collection on each page load:

```python
data = list(db.tweets.find())
```

---

## 9. Component 5 — Django Dashboard (Visualization)

### 9.1 Project Structure

```
Django-Dashboard/
├── BigDataProject/          ← Django project config
│   ├── settings.py          ← App settings, static files config
│   ├── urls.py              ← Root URL dispatcher
│   ├── static/imgs/         ← Generated chart images stored here
│   ├── wsgi.py
│   └── asgi.py
├── dashboard/               ← Main Django app
│   ├── views.py             ← Core logic: fetch from MongoDB, generate charts
│   ├── consumer_user.py     ← Loads PySpark model for user text classification
│   ├── urls.py              ← Routes: "/" → dashboard, "/classify/" → classify
│   ├── models.py            ← Minimal (MongoDB used directly, not Django ORM)
│   └── migrations/
├── templates/
│   ├── dashboard/index.html ← Main dashboard template
│   └── dashboard/classify.html ← Text classification page template
├── logistic_regression_model.pkl/ ← Saved PySpark pipeline
├── manage.py
└── db.sqlite3               ← Default Django admin/session DB (not used for tweets)
```

### 9.2 URL Routes

| URL | View Function | Description |
|---|---|---|
| `/` | `dashboard()` | Main real-time sentiment dashboard |
| `/classify/` | `classify()` | Manual text sentiment classification |
| `/admin/` | Django Admin | Admin panel |

### 9.3 Dashboard View (`views.dashboard`)

**Step-by-step execution when a user visits `http://127.0.0.1:8000/`:**

1. **Fetch all data from MongoDB:**
   ```python
   data = list(db.tweets.find())
   ```

2. **Count sentiments:**
   ```python
   sentiment_counts = {'Negative': 0, 'Positive': 0, 'Neutral': 0, 'Irrelevant': 0}
   for entry in data:
       sentiment_counts[entry['prediction']] += 1
   ```

3. **Calculate percentage rates:**
   ```python
   sentiment_rates = {
       label: round((count / len_data) * 100, 2)
       for label, count in sentiment_counts.items()
   }
   ```

4. **Generate word frequency chart** (`plot_word_frequencies_per_class`):
   - Extracts the top 5 most frequent words per sentiment class.
   - Uses NLTK tokenization and stopword removal.
   - Draws a grouped bar chart with `matplotlib`.
   - Saves it as a PNG to `BigDataProject/static/imgs/Word_Frequencies_for_all_classes.png`.

5. **Render the template** with context containing:
   - `data` — full list of tweet documents
   - `len_data` — total tweet count
   - `sentiment_counts` — raw counts per label
   - `sentiment_rates` — percentage rates per label

### 9.4 Classify View (`views.classify`)

**When a user visits `http://127.0.0.1:8000/classify/` and submits a tweet:**

1. Receives the tweet text from the POST form.
2. Calls `classify_text(text)` from `consumer_user.py`.
3. Inside `classify_text`:
   - Creates a Spark DataFrame with a single row.
   - Runs the full PySpark pipeline (Tokenizer → StopWordsRemover → CountVectorizer → LogisticRegression).
   - Returns the sentiment label string.
4. Displays the prediction result back to the user.

### 9.5 Word Frequency Visualization Logic

```python
def plot_word_frequencies_per_class(data):
    # Group words by sentiment class
    for tweet, prediction in result_tuples:
        class_texts[prediction].extend(preprocess_text(tweet))
    
    # For each class, count top 5 words
    for class_label, words in all_words_per_class.items():
        top_words = Counter(words).most_common(5)
        # Plot bar chart with class-specific color
```

Color mapping:
- `Negative` → Red
- `Positive` → Blue
- `Neutral` → Green
- `Irrelevant` → Purple

---

## 10. Data Flow — End to End

Here is the complete step-by-step data flow:

```
Step 1: CSV File (twitter_validation.csv)
         ↓
         [Kafka Producer] reads row by row
         ↓
Step 2: Message published to Kafka Topic "twitter"
         Message format: "tweet_id, entity, sentiment, tweet_text"
         ↓
Step 3: [Spark Structured Streaming Consumer] reads micro-batches
         ↓
Step 4: Text Preprocessing
         - Remove URLs, @mentions, #hashtags
         - Lowercase
         - Remove non-alpha characters
         ↓
Step 5: PySpark ML Pipeline applied
         [Tokenizer] → tokens
         [StopWordsRemover] → filtered tokens
         [CountVectorizer] → sparse feature vector
         [LogisticRegression] → prediction (0,1,2,3)
         ↓
Step 6: Map prediction index → label string
         0→Negative, 1→Positive, 2→Neutral, 3→Irrelevant
         ↓
Step 7: Write to MongoDB
         Database: bigdata_project | Collection: tweets
         Document: { tweet: "...", prediction: "Positive" }
         ↓
Step 8: [Django Dashboard] — User opens browser
         - Fetches all documents from MongoDB
         - Computes sentiment counts and rates
         - Generates word frequency bar chart (matplotlib)
         - Renders HTML template with Chart.js + statistics
         ↓
Step 9: User sees real-time sentiment results in browser
```

---

## 11. File & Directory Structure

```
Real-Time-Twitter-Sentiment-Analysis-main/
│
├── zk-single-kafka-single.yml        ← Docker Compose for Kafka + ZooKeeper
├── requirements.txt                  ← Python dependencies
├── README.md                         ← Project README
├── bigdataproject rapport.pdf        ← Project report (in French)
│
├── Kafka-PySpark/                    ← [NOT in this zip] Kafka producer & Spark consumer
│   ├── producer-validation-tweets.py ← Reads CSV → publishes to Kafka
│   ├── consumer-pyspark.py           ← Kafka consumer → Spark ML → MongoDB
│   └── twitter_validation.csv        ← Input dataset for producer
│
├── ML PySpark Model/                 ← [NOT in this zip] Jupyter notebooks
│   └── *.ipynb                       ← Model training notebooks
│
└── Django-Dashboard/                 ← Django web application
    ├── manage.py                     ← Django CLI entry point
    ├── db.sqlite3                    ← SQLite (Django admin only)
    ├── logistic_regression_model.pkl/← Saved PySpark PipelineModel
    │   ├── metadata/
    │   └── stages/
    │       ├── 0_Tokenizer_.../
    │       ├── 1_StopWordsRemover_.../
    │       └── 2_CountVectorizer_.../
    ├── BigDataProject/               ← Django project config package
    │   ├── settings.py
    │   ├── urls.py
    │   ├── static/imgs/              ← Generated chart PNGs saved here
    │   ├── wsgi.py
    │   └── asgi.py
    └── dashboard/                    ← Django app
        ├── views.py                  ← Core view logic
        ├── consumer_user.py          ← PySpark inference for /classify
        ├── urls.py                   ← Route definitions
        ├── models.py                 ← Placeholder (MongoDB used directly)
        ├── admin.py
        └── migrations/
```

---

## 12. Configuration Files

### 12.1 `zk-single-kafka-single.yml`

Docker Compose file that sets up the message broker infrastructure:

```yaml
version: '2.1'
services:
  zoo1:
    image: confluentinc/cp-zookeeper:7.3.0
    ports: ["2181:2181"]
    environment:
      ZOOKEEPER_CLIENT_PORT: 2181

  kafka1:
    image: confluentinc/cp-kafka:7.3.0
    ports:
      - "9092:9092"    # External (producer/consumer)
      - "29092:29092"  # Docker internal
      - "9999:9999"    # JMX monitoring
    environment:
      KAFKA_ZOOKEEPER_CONNECT: zoo1:2181
      KAFKA_BROKER_ID: 1
      KAFKA_OFFSETS_TOPIC_REPLICATION_FACTOR: 1
    depends_on:
      - zoo1
```

### 12.2 `Django-Dashboard/BigDataProject/settings.py`

Key Django configuration:

```python
INSTALLED_APPS = [
    ...,
    'dashboard',   # The main sentiment dashboard app
]

# SQLite for Django admin/sessions (not for tweet data!)
DATABASES = {
    'default': {
        'ENGINE': 'django.db.backends.sqlite3',
        'NAME': BASE_DIR / 'db.sqlite3',
    }
}

# Static files (CSS, JS, generated charts)
STATIC_URL = 'static/'
STATIC_ROOT = os.path.join(BASE_DIR, 'static')
STATICFILES_DIRS = [
    os.path.join(BASE_DIR, 'BigDataProject/static'),
]
```

> **Note:** MongoDB is connected directly via `PyMongo` in `views.py`, NOT through Django's ORM/database backends.

---

## 13. Python Dependencies

From `requirements.txt`:

| Package | Version | Purpose |
|---|---|---|
| `Django` | 5.0.6 | Web framework for dashboard |
| `matplotlib` | 3.8.3 | Server-side chart image generation |
| `nltk` | 3.8.1 | Tokenization & stopword removal |
| `pandas` | 2.2.2 | DataFrame operations in chart views |
| `pymongo` | 3.12.1 | MongoDB connection from Python |
| `pyspark` | 3.5.1 | Spark ML pipeline + Streaming |
| `seaborn` | 0.13.2 | Statistical plot styling |
| `kafka` | 1.3.5 | Kafka producer Python client |

Install with:
```bash
pip install -r requirements.txt
```

---

## 14. How to Run the Project

### Step 1 — Start Kafka (Docker)

```bash
docker-compose -f zk-single-kafka-single.yml up -d
```

This starts ZooKeeper (port 2181) and Kafka broker (port 9092).

### Step 2 — Create the Kafka Topic

Enter the Kafka container and create the topic:

```bash
docker exec -it kafka1 /bin/bash
kafka-topics --create --topic twitter --bootstrap-server localhost:9092
kafka-topics --describe --topic twitter --bootstrap-server localhost:9092
```

### Step 3 — Start MongoDB

```bash
# Linux/Mac:
sudo systemctl start mongod
# Or use MongoDB Compass GUI
```

### Step 4 — Run the Kafka Producer

Navigate to the `Kafka-PySpark/` folder and run:

```bash
py producer-validation-tweets.py
```

This starts reading `twitter_validation.csv` and publishes tweets to the `twitter` Kafka topic in real time.

### Step 5 — Run the Spark Streaming Consumer

In another terminal, from `Kafka-PySpark/`:

```bash
py consumer-pyspark.py
```

This reads from Kafka, applies the ML model, and writes results to MongoDB.

### Step 6 — Run the Django Dashboard

Navigate to `Django-Dashboard/`:

```bash
python manage.py collectstatic
python manage.py runserver
```

Open your browser and go to:
- **Dashboard:** `http://127.0.0.1:8000/`
- **Classify page:** `http://127.0.0.1:8000/classify/`

---

## 15. Summary

This project demonstrates a complete **Big Data real-time pipeline** with the following key ideas:

| Concept | Implementation |
|---|---|
| **Real-time ingestion** | Kafka producer reads CSV → publishes to topic `twitter` |
| **Stream processing** | Spark Structured Streaming consumes Kafka topic in micro-batches |
| **ML inference at scale** | Pre-trained PySpark MLlib pipeline (Tokenizer → StopWords → CountVectorizer → LogisticRegression) |
| **NoSQL persistence** | MongoDB stores tweet+prediction documents |
| **Interactive visualization** | Django dashboard fetches MongoDB data, generates matplotlib charts, renders with Chart.js |
| **Manual classification** | `/classify/` endpoint loads same PySpark model to classify user-provided text |
| **Containerization** | Kafka + ZooKeeper run in Docker for easy setup |

The system is designed as a **loosely coupled architecture** — each component (producer, consumer, database, dashboard) is independent and communicates through well-defined interfaces (Kafka topics, MongoDB collections, HTTP endpoints).

---

*Documentation generated for the Real-Time Twitter Sentiment Analysis project.*
