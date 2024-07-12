import os

from connector import MongoDbConnector
from obsrv.connector.batch import SourceConnector

# from obsrv.utils import Config

if __name__ == "__main__":
    connector = MongoDbConnector()
    config_file_path = os.path.join(os.path.dirname(__file__), "config/config.yaml")

    SourceConnector.process(connector=connector, config_file_path=config_file_path)
