import os
import yaml
from typing import Any, Dict, Iterator
import time, datetime

from pyspark.sql import SparkSession, DataFrame

from obsrv.common import ObsrvException
from obsrv.connector import ConnectorContext, MetricsCollector
from obsrv.connector.batch import ISourceConnector
from obsrv.models import ExecutionState, StatusCode
from obsrv.utils import LoggerController
from pyspark.conf import SparkConf

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
        self.config_file_path = os.path.join(os.path.dirname(__file__), "config/config.yaml")
        self.load_config()

    def load_config(self):
        try:
            with open(self.config_file_path, 'r') as f:
                config = yaml.safe_load(f)
                mongo_config = config.get("mongo", {})
                self.dbname = mongo_config.get("dbname")
                self.port = mongo_config.get("port")
                self.host = mongo_config.get("host")
                self.collection = mongo_config.get("collection")
                self.uri = f"mongodb://{self.host}:{self.port}/{self.dbname}.{self.collection}"
        except Exception as e:
            logger.error(f"Error loading MongoDB configuration: {e}")

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

        ctx.state.put_state("status", self.running_state)
        ctx.state.save_state()
        self.max_retries = (
            connector_config["source"]["max_retries"]
            if "max_retries" in connector_config["source"]
            else MAX_RETRY_COUNT
        )
        # self._get_provider(connector_config)
        self._get_documents_to_process(sc)
        for res in self._process_documents(sc):
            yield res

        last_run_time = datetime.datetime.now()
        ctx.state.put_state("status", self.not_running_state)
        ctx.state.put_state("last_run_time", last_run_time)
        ctx.state.save_state()
    def get_spark_conf(self, connector_config) -> SparkConf:
        # You can customize Spark configuration here if needed
        return self.get_spark_config(connector_config)

    def get_spark_config(self, connector_config) -> SparkConf:
        
        conf = SparkConf()
        conf.setAppName("MongoDBConnector")
        conf.set("spark.jars.packages", "org.mongodb.spark:mongo-spark-connector_2.12:3.0.1")
        return conf
    def _get_spark_session(self):
        return SparkSession.builder.config(conf=self.get_spark_config()).getOrCreate()

    def _get_documents_to_process(self, sc: SparkSession) -> None:
        try:
            self.documents = sc.read.format("mongo").option("uri", self.uri).load()
            self.documents.show() 
        except Exception as e:
            raise ObsrvException(f"Error fetching documents from MongoDB: {e}")

    def _process_documents(self, sc: SparkSession) -> Iterator[DataFrame]:
        try:
            if self.documents is None:
                raise ObsrvException("No documents found to process")

            for row in self.documents.rdd.toLocalIterator():
    
                yield sc.createDataFrame([row])

        except Exception as e:
            raise ObsrvException(f"Error processing documents: {e}")
