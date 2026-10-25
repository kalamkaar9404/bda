# Big Data Project: Real-Time Twitter Sentiment Analysis Using Kafka, Spark (MLLib & Streaming), MongoDB and Django.

## Overview

This repository contains a Big Data project focused on real-time sentiment analysis of Twitter data (classification of tweets). The project leverages various technologies to collect, process, analyze, and visualize sentiment data from tweets in real-time.

## Project Architecture

The project is built using the following components:

- **Apache Kafka**: Used for real-time data ingestion from Twitter DataSet.
- **Spark Streaming**: Processes the streaming data from Kafka to perform sentiment analysis.
- **MongoDB**: Stores the processed sentiment data.
- **Django**: Serves as the web framework for building a real-time dashboard to visualize the sentiment analysis results.
- **chart.js** & **matplotlib** : for plotting.

- This is the project plan :
   ![project img](imgs/flow.png)

## Features

- **Real-time Data Ingestion**: Collects live tweets using Kafka from the Twitter DataSet.
- **Stream Processing**: Utilizes Spark Streaming to process and analyze the data in real-time.
- **Sentiment Analysis**: Classifies tweets into different sentiment categories (positive, negative, neutral) using natural language processing (NLP) techniques.
- **Data Storage**: Stores the sentiment analysis results in MongoDB for persistence.
- **Visualization**: Provides a real-time dashboard built with Django to visualize the sentiment trends and insights.

## Data description:

In This Project I'm using a Dataset (twitter_training.csv and twitter_validation.csv) to create pyspark Model and for create live tweets using Kafka. Each line of the "twitter_training.csv" learning database represents a Tweet, it contains over 74682 lines;

The data types of Features are:
- Tweet ID: int
- Entity: string
- Sentiment: string (Target)
- Tweet content: string

The validation database “twitter_validation.csv” contains 998 lines (Tweets) with the same features of “twitter_training.csv”.

This is the Data Source:
https://www.kaggle.com/datasets/jp797498e/twitter-entity-sentiment-analysis

### Use a reviewed TweetClaw or X export

The Kafka producer reads `Kafka-PySpark/twitter_validation.csv`, which expects
four fields: tweet ID, entity, sentiment, and text. To test newer reviewed
tweet exports without hand-editing the CSV, convert them first:

```bash
python tools/normalize_tweet_export.py examples/tweetclaw-reviewed-export.csv \
  --output Kafka-PySpark/twitter_validation.csv
```

The converter accepts common Twitter/X and TweetClaw CSV headers such as
`tweet_id`, `account`, `sentiment`, `label`, `text`, `tweet`, `full_text`, and
`content`. Sentiment values must map to `Negative`, `Positive`, `Neutral`, or
`Irrelevant`. TweetClaw exports from https://github.com/Xquik-dev/tweetclaw are
best used after a review step adds one of those sentiment labels.

## Repository Structure

- **Django-Dashboard** : this folder contains Dashboard Django Application
- **Kafka-PySpark** : this folder contains kafka provider and pyspark streaming (kafka consumer).
- **ML PySpark Model** : this folder contains the trained model with jupyter notebook and datasets.
- **zk-single-kafka-single.yml** : Download and install Apache Kafka in docker.
- **bigdataproject rapport** : a brief report about the project (in french).


## Getting Started

### Prerequisites

| Requirement | Why | Notes |
|---|---|---|
| **Python 3.10 or 3.11** | Driver + Spark workers | PySpark 3.5 cannot use 3.12+ as a worker. Check with `py -0p`. |
| **JDK 8, 11 or 17** | Spark runs on the JVM | Spark 3.5 does **not** support newer JDKs. [Temurin 17](https://adoptium.net) is recommended. |
| **Docker Desktop** | Kafka, ZooKeeper, MongoDB | On Windows this needs the *Virtual Machine Platform* feature enabled. |

You do **not** need a separate Spark install (PySpark bundles it), a separate
MongoDB install (it runs in Docker), or `winutils.exe` (see *Windows notes*).

### Installation

1. **Clone the repository**:
   ```bash
   git clone https://github.com/drisskhattabi6/Real-Time-Twitter-Sentiment-Analysis.git
   cd Real-Time-Twitter-Sentiment-Analysis
   ```

2. **Run the setup script** — creates `.venv`, installs every dependency,
   builds the Windows filesystem shim and fetches the NLTK corpora:
   ```bash
   py -3.10 setup.py
   ```

   <details>
   <summary>Or do it by hand</summary>

   ```bash
   py -3.10 -m venv .venv
   .venv\Scripts\activate            # Linux/macOS: source .venv/bin/activate
   pip install -r requirements.txt
   python tools/build_barelocalfs.py  # Windows only
   ```
   </details>

3. **Train the model** (~2 minutes, writes `Django-Dashboard/logistic_regression_model.pkl`):
   ```bash
   python train_model.py
   ```
   The repository does not ship a usable trained model, so this step is
   required before the consumer or the `/classify` page will work.

4. **Start Kafka, ZooKeeper and MongoDB**:
   ```bash
   docker compose -f zk-single-kafka-single.yml up -d
   ```
   This also creates the `twitter` topic automatically — no manual
   `kafka-topics --create` step is needed.

### Windows notes

Hadoop implements file permissions on Windows by shelling out to
`winutils.exe`. Without it, Spark can read but **cannot write**, so saving the
trained model fails with `HADOOP_HOME and hadoop.home.dir are unset`.

Rather than requiring that third-party binary, this project ships a small
Hadoop `FileSystem` implementation that skips the permission calls
(`tools/barelocalfs/`). `python tools/build_barelocalfs.py` compiles it with
your JDK, and `spark_env.py` registers it automatically — but only on Windows,
and only when `HADOOP_HOME` is not already set, so a real winutils or a Linux
machine takes precedence.

`spark_env.py` also handles the two other things that silently break Spark:

- It points `JAVA_HOME` at a supported JDK if the current one is too new.
- It sets `PYSPARK_PYTHON` to the running interpreter, so the Spark workers
  match the driver. (A mismatch surfaces only as
  `SocketException: Connection reset`.)

### Running the Project

The pipeline is four processes. Three of them keep running, so use **three
terminals** plus Docker in the background:

```
            +-------------------------------------------------+
            |  Docker:  ZooKeeper  +  Kafka  +  MongoDB        |
            +-------------------------------------------------+
                 ^                  ^                  ^
                 | publishes        | consumes         | reads
                 | tweets           | + writes         | tweets
                 |                  |                  |
            Terminal 1          Terminal 2          Terminal 3
            producer     -->    consumer     -->    Django
            (CSV -> Kafka)      (classify ->        (dashboard
                                 MongoDB)            on :8000)
```

In **every** terminal, activate the virtual environment first:

```bash
.venv\Scripts\activate
```

On Linux/macOS:

```bash
source .venv/bin/activate
```

Your prompt should now start with `(.venv)`. Every `python` below refers to
that environment.

---

#### Step 1 - Start Kafka, ZooKeeper and MongoDB

```bash
docker compose -f zk-single-kafka-single.yml up -d
```

First run pulls the images (a few hundred MB) and takes a couple of minutes.

**Verify** all four containers are up:

```bash
docker compose -f zk-single-kafka-single.yml ps
```

```
NAME         IMAGE                             STATUS
kafka1       confluentinc/cp-kafka:7.3.0       Up
kafka-init   confluentinc/cp-kafka:7.3.0       Exited (0)
mongo        mongo:7                           Up
zoo1         confluentinc/cp-zookeeper:7.3.0   Up
```

`kafka-init` **should** show `Exited (0)` - it is a one-shot job that creates
the `twitter` topic and then stops. Confirm the topic exists:

```bash
docker exec kafka1 kafka-topics --bootstrap-server localhost:19092 --list
```

You should see `twitter` in the output.

---

#### Step 2 - Train the sentiment model

Skip this if `Django-Dashboard/logistic_regression_model.pkl/stages/` already
exists. Otherwise, from the project root:

```bash
python train_model.py
```

Takes roughly 2 minutes. Expected output:

```
[OK] SparkSession started - Spark 3.5.4
[OK] Loaded 73,996 training rows
+----------+-----+
| Sentiment|count|
+----------+-----+
|Irrelevant|12875|
|  Negative|22358|
|   Neutral|18108|
|  Positive|20655|
+----------+-----+
[OK] Training complete!
[OK] Training accuracy: 82.37%
[OK] Model saved successfully!
```

**Verify** the model is complete - it must contain four stage folders:

```bash
ls Django-Dashboard/logistic_regression_model.pkl/stages
```

```
0_Tokenizer_...  1_StopWordsRemover_...  2_CountVectorizer_...  3_LogisticRegression_...
```

A model folder containing only `metadata/` is incomplete and will not load.

---

#### Step 3 - Terminal 1: start the Kafka producer

```bash
python Kafka-PySpark/producer-validation-tweets.py
```

It reads `Kafka-PySpark/twitter_validation.csv` and publishes one tweet every
0.5 s (1000 tweets, about 8 minutes) to the `twitter` topic:

```
[Producer] Connected to Kafka at localhost:9092
[Producer] Publishing to topic: 'twitter'
------------------------------------------------------------
[1] Sent -> 3364,Facebook,Irrelevant,I mentioned on Facebook that I was struggling...
[2] Sent -> 352,Amazon,Neutral,BBC News - Amazon boss Jeff Bezos rejects claims...
```

Leave it running. `Ctrl+C` stops it early and reports how many were sent.

> To stream your own reviewed export instead, convert it first:
> `python tools/normalize_tweet_export.py my-export.csv --output Kafka-PySpark/twitter_validation.csv`

---

#### Step 4 - Terminal 2: start the PySpark consumer

```bash
python Kafka-PySpark/consumer-pyspark.py
```

It starts Spark, loads the model, connects to MongoDB and Kafka, then
classifies tweets in batches of 20:

```
[1/4] Starting SparkSession...
     Spark 3.5.4 started.
[2/4] Loading ML model...
     Model loaded from: ...\Django-Dashboard\logistic_regression_model.pkl
[3/4] Connecting to MongoDB...
     Connected -> bigdata_project.tweets
[4/4] Connecting to Kafka...
     Listening on topic 'twitter' at localhost:9092
------------------------------------------------------------
Streaming tweets... Press Ctrl+C to stop.

[Batch] Classified & stored 20 tweets | Total: 20
[Batch] Classified & stored 20 tweets | Total: 40
```

Each of the four startup steps fails with an explicit message naming the fix,
so the step number tells you what is wrong. `Ctrl+C` flushes the partial batch
before exiting.

**Verify** the data is landing in MongoDB:

```bash
docker exec mongo mongosh --quiet --eval "db.getSiblingDB('bigdata_project').tweets.countDocuments()"
```

The number should grow each time you run it.

---

#### Step 5 - Terminal 3: start the Django dashboard

```bash
cd Django-Dashboard
python manage.py migrate
python manage.py collectstatic --noinput
python manage.py runserver
```

The `migrate` and `collectstatic` steps are only needed on the first run.

```
Starting development server at http://127.0.0.1:8000/
```

Open **`http://127.0.0.1:8000`**. The dashboard shows:

- total tweets processed, and the percentage per sentiment class;
- a **pie chart** and a **bar chart** of the distribution (Chart.js);
- a **top-words-per-class** bar chart, rendered server-side by matplotlib into
  `BigDataProject/static/imgs/Word_Frequencies_for_all_classes.png`;
- a table of every classified tweet with its label.

Refresh the page to pick up new tweets - the consumer keeps writing while the
producer runs.

Open **`http://127.0.0.1:8000/classify/`** to classify your own text. The first
submission takes ~20 s while Spark starts; later ones are fast, because the
session and the model are cached for the life of the server.

---

#### Stopping everything

`Ctrl+C` in each of the three terminals, then stop the containers:

```bash
docker compose -f zk-single-kafka-single.yml down
```

Add `-v` to also delete the stored tweets:

```bash
docker compose -f zk-single-kafka-single.yml down -v
```

#### Starting over with a clean database

```bash
docker exec mongo mongosh --quiet --eval "db.getSiblingDB('bigdata_project').tweets.drop()"
```

The consumer tracks its position in Kafka under the group id
`pyspark-sentiment-group`, so after clearing MongoDB it resumes from where it
left off rather than re-reading old tweets. To replay everything from the
start, reset the stack with `down -v` and bring it back up.

---

#### Quick reference

| Command | What it does |
|---|---|
| `py -3.10 setup.py` | Full first-time setup (venv, deps, shim, NLTK data) |
| `python train_model.py` | Train and save the PySpark MLlib model |
| `docker compose -f zk-single-kafka-single.yml up -d` | Start Kafka, ZooKeeper, MongoDB |
| `docker compose -f zk-single-kafka-single.yml ps` | Check the containers |
| `python Kafka-PySpark/producer-validation-tweets.py` | Stream the CSV into Kafka |
| `python Kafka-PySpark/consumer-pyspark.py` | Classify tweets into MongoDB |
| `python manage.py runserver` | Start the dashboard (from `Django-Dashboard/`) |
| `python tools/build_barelocalfs.py` | Rebuild the Windows filesystem shim |
| `docker compose -f zk-single-kafka-single.yml down` | Stop everything |

### Troubleshooting

| Symptom | Cause | Fix |
|---|---|---|
| `SocketException: Connection reset` during a Spark job | Worker Python differs from the driver's | Run via the project venv; `spark_env.py` handles the rest |
| `HADOOP_HOME and hadoop.home.dir are unset` on save | winutils missing on Windows | `python tools/build_barelocalfs.py` |
| `UnsupportedClassVersionError` / odd JVM errors | JDK newer than 17 | Install Temurin 17; `spark_env.py` will pick it up |
| `ModuleNotFoundError: No module named 'kafka'` | The PyPI package `kafka` (1.3.5) is a dead Python-2 mirror | `pip install kafka-python` (already in `requirements.txt`) |
| Dashboard says MongoDB is unreachable | Containers not running | `docker compose -f zk-single-kafka-single.yml up -d` |
| `No trained model ... 'stages/' directory is missing` | Model not trained yet | `python train_model.py` |
| `wsl: Virtual Machine Platform is not enabled` | Docker cannot start on Windows | Enable it in *Windows Features* (admin), then reboot |

## More informations : 

- Django Dashboard get the data from MongoDb DataBase.
- the User can classify his owne text in `http://127.0.0.1:8000/classify` link.
- in the Dashboard, There is a table contains tweets with labels.
- in the Dashboard, There is 3 statistics or plots : labels rates - pie plot - bar plot.


## Team :

- [Khattabi Idriss](https://github.com/drisskhattabi6) 
- [Boufarhi Ayman](https://github.com/aymanboufarhi) 
- [Abdelali IBN TABET](https://github.com/abd-ibn)

## Supervised By : 

- Prof. **Yasyn El Yusufi**

---

Abdelmalek Essaadi University - Faculty of Sciences and Technology of Tangier

- Master: Artificial Intelligence and Data Science
- Module: Big Data

---

- By following the above instructions, you should be able to set up and run the real-time Twitter sentiment analysis project on your local machine. Happy coding!

- Feel free to explore the project and customize it according to your requirements. If you encounter any issues or have any questions, don't hesitate to reach out!
