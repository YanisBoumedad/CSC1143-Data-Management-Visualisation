import os
from datetime import datetime

GCP_PROJECT_ID = "regal-sun-478114-q5"
GCP_REGION = "europe-west1"
GCP_ZONE = "europe-west1-b"

GCS_BUCKET_NAME = "csc1142-projet"
GCS_RAW_PATH = "raw"
GCS_PROCESSED_PATH = "processed"
GCS_SCRIPTS_PATH = "scripts"
GCS_RESULTS_PATH = "results"

DATAPROC_CLUSTER_NAME = "csc1142-spark-cluster"
DATAPROC_REGION = GCP_REGION

API_URLS = {
    "carburants": "https://data.economie.gouv.fr/api/explore/v2.1/catalog/datasets/prix-carburants-quotidien/exports/json"
}

LOG_FORMAT = '%(asctime)s - %(name)s - %(levelname)s - %(message)s'
LOG_LEVEL = "INFO"

def get_timestamp():
    return datetime.now().strftime("%Y%m%d_%H%M%S")

def get_date():
    return datetime.now().strftime("%Y-%m-%d")

def get_filename(dataset_name, extension="json"):
    return f"{dataset_name}_{get_date()}.{extension}"
