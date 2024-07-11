import os,yaml
from pymongo import MongoClient
from pyspark.sql import SparkSession



class mongo:
    def uri():
        config_file_path = os.path.join(os.path.dirname(__file__), "config/config.yaml")

        config = yaml.safe_load(open(config_file_path))
        # print(config)

        dbname=config["mongo"]["dbname"]
        port=config["mongo"]["port"]
        host=config["mongo"]["host"]
        collection=config["mongo"]["collection"]
        uri=f"mongodb://{host}:{port}/{dbname}.{collection}"
        # print("uri",uri)
        return uri
    
    def get_spark_session():
        spark = SparkSession.builder \
        .appName("MongoDB Integration") \
        .config("spark.mongodb.input.uri", app.uri()) \
        .config("spark.jars.packages", "org.mongodb.spark:mongo-spark-connector_2.12:3.0.1") \
        .getOrCreate()
        return spark
      
        
    def read_doc():
        client = MongoClient(app.uri())
        print(client)
        spark=app.get_spark_session()
        df = spark.read.format("mongo").load()
        df.show()
        return df

    
        
        

app=mongo
app.read_doc()
app.get_spark_session()