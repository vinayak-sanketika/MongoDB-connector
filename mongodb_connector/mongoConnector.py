

from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from kafka import KafkaProducer
import json

# Initialize SparkSession
spark = SparkSession.builder \
    .appName("MongoDB to Kafka with Spark") \
    .config("spark.jars.packages", "org.mongodb.spark:mongo-spark-connector_2.12:3.0.1") \
    .getOrCreate()

# MongoDB connection string and database/collection name
mongo_uri = "mongodb://localhost:27017/"
mongo_database = "sample_fruit"
mongo_collection = "fruits"

# Kafka broker(s) and topic
kafka_servers = ['localhost:9092']
kafka_topic = 'mongoTopic'

# Function to send each partition to Kafka
def send_partition_to_kafka(partition):
    # Initialize Kafka producer
    producer = KafkaProducer(bootstrap_servers=kafka_servers,
                             value_serializer=lambda v: json.dumps(v).encode('utf-8'))

    # Send each row in the partition to Kafka
    for row in partition:
        try:
            # Convert row to dictionary (adjust as per your DataFrame structure)
            record = row.asDict()
            # Send record to Kafka
            producer.send(kafka_topic, value=record)
        except Exception as e:
            print(f"Error processing row: {row}. Error: {e}")

    # Flush and close Kafka producer for this partition
    producer.flush()
    producer.close()

try:
    # Read from MongoDB into a DataFrame
    df = spark.read.format("mongo") \
        .option("uri", mongo_uri + mongo_database + "." + mongo_collection) \
        .load()

    # Show the DataFrame schema and data (for testing purposes)
    df.printSchema()
    df.show()

    # Convert DataFrame to RDD of Rows and send each partition to Kafka
    df.foreachPartition(send_partition_to_kafka)

finally:
    spark.stop()
