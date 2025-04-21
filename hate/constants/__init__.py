import os

from datetime import datetime

# Common constants
TIMESTAMP: str = datetime.now().strftime("%m_%d_%Y_%H_%M_%S")
ARTIFACTS_DIR = os.path.join("artifacts", TIMESTAMP)
BUCKET_NAME = 'hatespeechnlp_proj'
ZIP_FILE_NAME = 'dataset.zip'
LABEL = 'label'
TWEET = 'tweet'
SCHEMA_FILE_PATH = os.path.join("config", "schema.yaml")

# Data ingestion constants
DATA_INGESTION_ARTIFACTS_DIR = "DataIngestionArtifacts"
DATA_INGESTION_IMBALANCE_DATA_DIR = "imbalanced_data.csv"
DATA_INGESTION_RAW_DATA_DIR = "raw_data.csv"

#Data Validation constants
DATA_VALIDATION_DIR_NAME: str = "DataValidation"
DATA_VALIDATION_REPORT_FILE_NAME: str = "report.yaml"