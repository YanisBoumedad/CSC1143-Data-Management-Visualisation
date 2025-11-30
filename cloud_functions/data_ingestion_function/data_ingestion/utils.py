import logging
import json
import csv
from io import StringIO
from datetime import datetime
from google.cloud import storage
from config import settings

logging.basicConfig(
    level=getattr(logging, settings.LOG_LEVEL),
    format=settings.LOG_FORMAT
)
logger = logging.getLogger(__name__)


class GCSUploader:
    def __init__(self, bucket_name=settings.GCS_BUCKET_NAME):
        self.bucket_name = bucket_name
        self.client = storage.Client(project=settings.GCP_PROJECT_ID)
        self.bucket = self.client.bucket(bucket_name)
        logger.info(f"GCS Uploader initialisé pour le bucket: {bucket_name}")

    def upload_json(self, data, destination_path, indent=2):
        try:
            blob = self.bucket.blob(destination_path)
            json_data = json.dumps(data, indent=indent, ensure_ascii=False)
            blob.upload_from_string(
                json_data,
                content_type='application/json'
            )
            uri = f"gs://{self.bucket_name}/{destination_path}"
            logger.info(f"JSON uploadé avec succès: {uri}")
            return uri
        except Exception as e:
            logger.error(f"Erreur lors de l'upload JSON: {str(e)}")
            raise

    def upload_csv(self, data, destination_path):
        try:
            blob = self.bucket.blob(destination_path)

            if isinstance(data, bytes):
                blob.upload_from_string(data, content_type='text/csv')
            else:
                blob.upload_from_string(
                    data.encode('utf-8'),
                    content_type='text/csv'
                )

            uri = f"gs://{self.bucket_name}/{destination_path}"
            logger.info(f"CSV uploadé avec succès: {uri}")
            return uri
        except Exception as e:
            logger.error(f"Erreur lors de l'upload CSV: {str(e)}")
            raise

    def upload_file(self, local_path, destination_path):
        try:
            blob = self.bucket.blob(destination_path)
            blob.upload_from_filename(local_path)
            uri = f"gs://{self.bucket_name}/{destination_path}"
            logger.info(f"Fichier uploadé avec succès: {uri}")
            return uri
        except Exception as e:
            logger.error(f"Erreur lors de l'upload du fichier: {str(e)}")
            raise

    def list_files(self, prefix):
        blobs = self.client.list_blobs(self.bucket_name, prefix=prefix)
        return [blob.name for blob in blobs]


def create_metadata(dataset_name, record_count, source_url, file_size_mb=None):
    metadata = {
        "dataset_name": dataset_name,
        "ingestion_timestamp": datetime.now().isoformat(),
        "ingestion_date": settings.get_date(),
        "source_url": source_url,
        "record_count": record_count,
        "gcp_project": settings.GCP_PROJECT_ID,
        "gcs_bucket": settings.GCS_BUCKET_NAME
    }

    if file_size_mb:
        metadata["file_size_mb"] = file_size_mb

    return metadata


def save_metadata(uploader, metadata, destination_path):
    metadata_path = destination_path.replace('.json', '_metadata.json').replace('.csv', '_metadata.json')
    return uploader.upload_json(metadata, metadata_path)


def log_ingestion_summary(dataset_name, record_count, gcs_uri, duration_seconds):
    logger.info("=" * 80)
    logger.info(f"  RÉSUMÉ INGESTION - {dataset_name}")
    logger.info(f"   • Enregistrements: {record_count:,}")
    logger.info(f"   • Destination GCS: {gcs_uri}")
    logger.info(f"   • Durée: {duration_seconds:.2f}s")
    logger.info("=" * 80)
