from pyspark.ml import PipelineModel
import re
import nltk
from kafka import KafkaConsumer
from json import loads
from pymongo import MongoClient
from pyspark.sql import SparkSession

# Establish connection to MongoDB
client = MongoClient('localhost', 27017)
db = client['bigdata_project']
collection = db['tweets']

# Download stopwords
nltk.download('stopwords', quiet=True)
nltk.download('punkt', quiet=True)

# Create a SparkSession
spark = SparkSession.builder \
    .appName("TwitterSentimentAnalysis") \
    .master("local[*]") \
    .getOrCreate()
spark.sparkContext.setLogLevel("WARN")

# --- THIS IS THE CORRECT WAY TO LOAD A SPARK MODEL ---
# It loads the model from the folder located in the same directory as the script.
pipeline = PipelineModel.load("logistic_regression_model.pkl")

def clean_text(text):
    if text is not None:
        text = re.sub(r'https?://\S+|www\.\S+|\.com\S+|youtu\.be/\S+', '', text)
        text = re.sub(r'(@|#)\w+', '', text)
        text = text.lower()
        text = re.sub(r'[^a-zA-Z\s]', '', text)
        text = re.sub(r'\s+', ' ', text).strip()
        return text
    else:
        return ''

class_index_mapping = { 0: "Negative", 1: "Positive", 2: "Neutral", 3: "Irrelevant" }

# Kafka Consumer
consumer = KafkaConsumer(
    'numtest',
    bootstrap_servers=['localhost:9092'],
    auto_offset_reset='earliest',
    enable_auto_commit=True,
    group_id='my-group',
    value_deserializer=lambda x: loads(x.decode('utf-8')))

for message in consumer:
    tweet = message.value[-1]
    preprocessed_tweet = clean_text(tweet)

    # --- THIS IS THE CORRECT WAY TO PREDICT WITH A SPARK MODEL ---
    # Create a Spark DataFrame to make a prediction
    data = spark.createDataFrame([(preprocessed_tweet,)], ["Text"])
    
    # Apply the pipeline to the new text
    processed_validation = pipeline.transform(data)
    prediction = processed_validation.collect()[0]['prediction']

    print("-> Tweet:", tweet)
    print("-> Preprocessed Tweet:", preprocessed_tweet)
    print("-> Predicted Sentiment Label:", class_index_mapping[int(prediction)])

    # Prepare document to insert into MongoDB
    tweet_doc = {
        "tweet": tweet,
        "prediction": class_index_mapping[int(prediction)]
    }

    # Insert document into MongoDB collection
    collection.insert_one(tweet_doc)

    print("/"*50)