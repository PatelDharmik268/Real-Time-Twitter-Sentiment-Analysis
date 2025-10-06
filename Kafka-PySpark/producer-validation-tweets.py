import csv
from time import sleep
from kafka import KafkaProducer
import json

# -------------------------
# Kafka Producer Setup
# -------------------------
producer = KafkaProducer(
    bootstrap_servers=['localhost:9092'],
    value_serializer=lambda x: json.dumps(x).encode('utf-8')
)

# -------------------------
# Read CSV and send to Kafka
# -------------------------
csv_file_path = 'twitter_validation.csv'  # make sure this path is correct

with open(csv_file_path, 'r', encoding='utf-8') as file_obj:
    reader_obj = csv.reader(file_obj)
    for data in reader_obj:
        # Send each row (tweet) to Kafka topic 'numtest'
        producer.send('numtest', value=data)
        print("Sent:", data)
        sleep(3)  # optional delay between messages

print("All tweets sent successfully!")
