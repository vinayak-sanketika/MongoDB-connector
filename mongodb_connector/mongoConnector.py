import os
import yaml
from typing import Any, Dict, Iterator
import time, datetime

from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.functions import col, desc

from obsrv.common import ObsrvException
from obsrv.connector import ConnectorContext, MetricsCollector
from obsrv.connector.batch import ISourceConnector
from obsrv.models import ExecutionState, StatusCode
from obsrv.utils import LoggerController
from pyspark.conf import SparkConf

from pymongo import *


logger = LoggerController(__name__)

MAX_RETRY_COUNT = 10


class MongoDbConnector(ISourceConnector):
    def __init__(self):
        self.provider = None
        self.objects = list()
        self.dedupe_tag = None
        self.success_state = StatusCode.SUCCESS.value
        self.error_state = StatusCode.FAILED.value
        self.running_state = ExecutionState.RUNNING.value
        self.not_running_state = ExecutionState.NOT_RUNNING.value
        self.queued_state = ExecutionState.QUEUED.value

        self.documents = None  # Initialize to None
        self.dbname = None
        self.port = None
        self.host = None
        self.collection = None
        self.uri = None
        self.uriDb = None

        self.batch_size = 100
        self.max_batch = 10

        self.last_run_time = None
        # self.last_document_time = None
    
    def process(
        self,
        sc: SparkSession,
        ctx: ConnectorContext,
        connector_config: Dict[Any, Any],
        metrics_collector: MetricsCollector,
    ) -> Iterator[DataFrame]:
        if (
            ctx.state.get_state("status", default_value=self.not_running_state)
            == self.running_state
        ):
            logger.info("Connector is already running. Skipping processing.")
            return
        self.last_runtime =ctx.state.get_state("last_run_time")
        # print("last_run_time",self.last_runtime)
        print("last_document_time",ctx.state.get_state("last_document_time"))
        self.last_document_time = ctx.state.get_state("last_document_time")

        ctx.state.put_state("status", self.running_state)
        ctx.state.save_state()
        self.max_retries = (
            connector_config["source_max_retries"]
            if "source_max_retries" in connector_config
            else MAX_RETRY_COUNT
        )
        # self._get_provider(connector_config)
        print("Connector Config from process function {0}".format(type(connector_config)))
        self.load_config(connector_config)
        # self._get_documents_to_process(sc)
        for res in self._process_documents(sc):
            yield res

        last_run_time = datetime.datetime.now()
        ctx.state.put_state("status", self.not_running_state)
        ctx.state.put_state("last_document_time", self.last_document_time)
        ctx.state.put_state("last_run_time", last_run_time)
        print("last_run_time",ctx.state.get_state("last_run_time"))

        ctx.state.save_state()
    
    def load_config(self, connector_config: Dict[Any, Any]):
        # self.connector_config = connector_config
        print(type(connector_config))

        self.dbname = connector_config["source_dbname"]
        self.collection = connector_config["source_collection"]
        self.host = connector_config["source_host"]
        self.port = connector_config["source_port"]
        self.uri = f"mongodb://{self.host}:{self.port}/{self.dbname}.{self.collection}"
        self.uriDb= f"mongodb://{self.host}:{self.port}/" 

        self.client = MongoClient(self.uriDb)
        
        self.db = self.client.get_database(self.dbname)
        self.collection = self.db.get_collection(self.collection)

        print("uri",self.uri)

        print("dbname:",self.dbname,"collection:",self.collection,"host",self.host,"port",self.port)


    def get_spark_conf(self, connector_config) -> SparkConf:
        return self.get_spark_config(connector_config)

    def get_spark_config(self, connector_config) -> SparkConf:
        
        conf = SparkConf()
        conf.setAppName("MongoDBConnector")
        conf.set("spark.jars.packages", "org.mongodb.spark:mongo-spark-connector_2.12:3.0.1")
        return conf
    
    def _get_spark_session(self):
        return SparkSession.builder.config(conf=self.get_spark_config()).getOrCreate()

    def _process_documents(self, sc: SparkSession) -> Iterator[DataFrame]:
        try:
            batch_number = 0
            
            while self.max_batch is None or batch_number < self.max_batch:
               
                query = {}
                if self.last_document_time is not None:
                    query = {"tpep_dropoff_datetime": {"$gt": self.last_document_time}}

                pipeline = [
                    {"$match": query}, 
                    {"$sort": {"tpep_dropoff_datetime": 1}}, 
                    {"$limit": self.batch_size} 
                ]

                documents = sc.read.format("mongo") \
                    .option("uri", self.uri) \
                    .option("pipeline", str(pipeline)) \
                    .load()
                
                documents.show(5)
                
                
                if documents.count() == 0:
                    break
                
                dropoff_times = documents.select("tpep_dropoff_datetime").orderBy(desc('tpep_dropoff_datetime')).head(1)
                
                if dropoff_times:
                    self.last_document_time = dropoff_times[0]['tpep_dropoff_datetime']
                    print("Updated last_document_time:", self.last_document_time)
                
                yield documents
                
                batch_number += 1

        except Exception as e:
            raise Exception(f"Error processing documents: {e}")