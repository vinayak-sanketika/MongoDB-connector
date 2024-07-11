

from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from kafka import KafkaProducer
import json


spark = SparkSession.builder \
    .appName("MongoDB to Kafka with Spark") \
    .config("spark.jars.packages", "org.mongodb.spark:mongo-spark-connector_2.12:3.0.1") \
    .getOrCreate()


mongo_uri = "mongodb://localhost:27017/"
mongo_database = "sample_fruit"
mongo_collection = "fruits"


kafka_servers = ['localhost:9092']
kafka_topic = 'mongoTopic'


def send_partition_to_kafka(partition):
    
    producer = KafkaProducer(bootstrap_servers=kafka_servers,
                             value_serializer=lambda v: json.dumps(v).encode('utf-8'))

    
    for row in partition:
        try:
            
            record = row.asDict()
            
            producer.send(kafka_topic, value=record)
        except Exception as e:
            print(f"Error processing row: {row}. Error: {e}")

    
    producer.flush()
    producer.close()

try:
    
    df = spark.read.format("mongo") \
        .option("uri", mongo_uri + mongo_database + "." + mongo_collection) \
        .load()

    
    df.printSchema()
    df.show()

    
    df.foreachPartition(send_partition_to_kafka)

finally:
    spark.stop()
