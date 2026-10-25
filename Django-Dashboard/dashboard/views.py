from django.shortcuts import render

from pymongo import MongoClient
from pymongo.errors import PyMongoError
import matplotlib
matplotlib.use('Agg')  # Use non-interactive backend
import matplotlib.pyplot as plt
import pandas as pd
from collections import Counter
import os
import re
import nltk
from nltk.tokenize import word_tokenize
from nltk.corpus import stopwords

# -----------------------------------------------------
# NLTK data. word_tokenize needs 'punkt_tab' from NLTK 3.9 onwards ('punkt' is
# kept for older installs). Downloads are quiet and skipped once present, so a
# missing network connection does not break the dashboard.
for _pkg in ('punkt', 'punkt_tab', 'stopwords'):
   try:
      nltk.download(_pkg, quiet=True)
   except Exception:
      pass

# -----------------------------------------------------

# Connect to MongoDB server. A short timeout keeps the page responsive when
# MongoDB is not running, instead of hanging for the 30s default.
client = MongoClient('mongodb://localhost:27017/', serverSelectionTimeoutMS=3000)
# Select the database
db = client['bigdata_project']

# -----------------------------------------------------

def clean_text(text):
    # Remove URLs using regular expression
   text = re.sub(r'http[s]?://(?:[a-zA-Z]|[0-9]|[$-_@.&+]|[!*\\(\\),]|(?:%[0-9a-fA-F][0-9a-fA-F]))+', '', text)
   # text = re.sub(r'\b(?:https?://|www\.)\S+\b', '', text)
   # Remove words starting with '@'
   text = re.sub(r'\b@\w+\b', '', text)
   # Remove non-alphanumeric characters
   text = re.sub(r'[^a-zA-Z0-9\s]', '', text)
   # Convert text to lowercase
   text = text.lower()
   text = text.strip()
   return text

def preprocess_text(text):

    # clean the text
    text = clean_text(text)

    # Tokenize the text
    tokens = word_tokenize(text)

    # Get English stopwords
    stop_words = set(stopwords.words('english'))

    # Remove stopwords
    filtered_tokens = [word for word in tokens if word.lower() not in stop_words]

    return filtered_tokens

class_list = ['Negative', 'Positive', 'Neutral', 'Irrelevant']

def plot_word_frequencies_per_class(data):
    keys_to_extract = ['tweet', 'prediction']
    result_tuples = [tuple(d[key] for key in keys_to_extract) for d in data]

    static_folder = os.path.join(os.path.dirname(os.path.dirname(__file__)), 'BigDataProject', 'static', 'imgs')
    os.makedirs(static_folder, exist_ok=True)

    all_words_per_class = {class_label: [] for class_label in class_list}
    color_map = {'Negative': 'red', 'Positive': 'blue', 'Neutral': 'green', 'Irrelevant': 'purple'}

    plt.figure(figsize=(12, 8))

    for tweet, prediction in result_tuples:
        # Ignore any label the model was not trained to produce.
        if prediction not in all_words_per_class:
            continue
        all_words_per_class[prediction].extend(preprocess_text(tweet))

    for class_label, class_words in all_words_per_class.items():
        word_freq = Counter(class_words)
        top_words = dict(word_freq.most_common(5))
        if not top_words:
            continue  # nothing classified into this class yet
        df = pd.DataFrame(list(top_words.items()), columns=['Word', 'Frequency'])
        plt.bar(df['Word'] + f' ({class_label})', df['Frequency'], color=color_map[class_label], alpha=0.7, label=class_label)

    plt.xlabel('Words')
    plt.ylabel('Frequency')
    #  plt.title('Top Word Frequencies for Each Class')
    plt.legend()
    plt.xticks(rotation=70)
    plt.tight_layout()

    plot_path = os.path.join(static_folder, 'Word_Frequencies_for_all_classes.png')
    plt.savefig(plot_path)
    plt.close()

# -----------------------------------------------------

def dashboard(request):
   # Fetch all data from MongoDB. If MongoDB is unreachable, render the empty
   # state with an explanation rather than a 500 page.
   try:
      data = list(db.tweets.find())
      db_error = ""
   except PyMongoError as exc:
      data = []
      db_error = f"Cannot reach MongoDB at localhost:27017 ({type(exc).__name__})."

   len_data = len(data)
   print("len_data : ", len_data)

   if len_data == 0:
      context = {'len_data': len_data, 'db_error': db_error}
      return render(request, 'dashboard/index.html', context)

   sentiment_counts = {label: 0 for label in class_list}
   for entry in data:
      prediction = entry.get('prediction')
      if prediction in sentiment_counts:
         sentiment_counts[prediction] += 1

   # Calculate the rate of each class
   sentiment_rates = {label: round((count / len_data) * 100, 2)  for label, count in sentiment_counts.items()}

   # A single list the template can loop over, so it never has to look a dict up
   # by a variable key (which Django templates cannot do).
   sentiment_stats = [
      {'label': label, 'count': sentiment_counts[label], 'rate': sentiment_rates[label]}
      for label in class_list
   ]

   plot_word_frequencies_per_class(data)

   context = {
      'data': data,
      'len_data': len_data,
      'sentiment_rates': sentiment_rates,
      'sentiment_counts': sentiment_counts,
      'sentiment_stats': sentiment_stats,
      'db_error': db_error,
   }
   return render(request, 'dashboard/index.html', context)


def classify(request) :

   error = False
   error_text = ""
   prediction = ""
   text = ""

   if request.method == 'POST':

      text = request.POST.get('text') or ""
      print(len(text.strip()))
      if len(text.strip()) > 0 :
         # Imported here so that a missing/untrained model does not stop the
         # whole site from loading.
         from .consumer_user import classify_text, ModelNotTrained
         try:
            prediction = classify_text(text)
         except ModelNotTrained as exc:
            error = True
            error_text = str(exc)
         except Exception as exc:
            error = True
            error_text = f"Classification failed: {type(exc).__name__}: {exc}"

      else :
         error = True
         error_text = "the Text is empty!! PLZ Enter Your Text."

   context = {
      "error" : error,
      "error_text" : error_text,
      "prediction" : prediction,
      "text" : text,
      "text_len" : len(text.strip())
   }

   print(context)
   return render(request, 'dashboard/classify.html', context)
