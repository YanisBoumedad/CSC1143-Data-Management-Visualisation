import requests
import logging
import time
import sys
import os
from datetime import datetime

sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

from data_ingestion.utils import GCSUploader, create_metadata, save_metadata, log_ingestion_summary
from config import settings

logger = logging.getLogger(__name__)


def fetch_carburants_data():
    url = settings.API_URLS["carburants"]
    logger.info(f"Récupération des données depuis: {url}")

    try:
        response = requests.get(url, timeout=60)
        response.raise_for_status()

        data = response.json()
        logger.info(f"{len(data)} enregistrements récupérés")
        return data

    except requests.exceptions.RequestException as e:
        logger.error(f"Erreur lors de la récupération des données: {str(e)}")
        raise


def process_carburants_data(raw_data):
    logger.info(" Traitement des données...")

    processed_data = []
    for record in raw_data:
        enriched_record = record.copy()
        enriched_record['ingestion_timestamp'] = datetime.now().isoformat()

        if 'date' in record:
            enriched_record['date'] = record['date']

        if 'cp' in record:
            enriched_record['cp'] = str(record['cp']).zfill(5)

        processed_data.append(enriched_record)

    logger.info(f"{len(processed_data)} enregistrements traités")
    return processed_data


def upload_to_gcs(data):
    uploader = GCSUploader()
    filename = settings.get_filename("carburants", "json")
    destination_path = f"{settings.GCS_RAW_PATH}/carburants/{filename}"

    logger.info(f"Upload vers GCS: {destination_path}")
    gcs_uri = uploader.upload_json(data, destination_path)

    metadata = create_metadata(
        dataset_name="carburants",
        record_count=len(data),
        source_url=settings.API_URLS["carburants"],
        file_size_mb=len(str(data)) / (1024 * 1024)
    )
    save_metadata(uploader, metadata, destination_path)

    return gcs_uri


def main():
    start_time = time.time()

    logger.info("=" * 80)
    logger.info("DÉMARRAGE INGESTION - PRIX DES CARBURANTS")
    logger.info("=" * 80)

    try:
        raw_data = fetch_carburants_data()
        processed_data = process_carburants_data(raw_data)
        gcs_uri = upload_to_gcs(processed_data)

        duration = time.time() - start_time
        log_ingestion_summary(
            dataset_name="Prix des Carburants",
            record_count=len(processed_data),
            gcs_uri=gcs_uri,
            duration_seconds=duration
        )

        return {
            "success": True,
            "gcs_uri": gcs_uri,
            "record_count": len(processed_data)
        }

    except Exception as e:
        logger.error(f"Échec de l'ingestion: {str(e)}")
        return {
            "success": False,
            "error": str(e)
        }


if __name__ == "__main__":
    result = main()
    if result["success"]:
        print(f"\nIngestion réussie: {result['record_count']} enregistrements")
        print(f"Fichier: {result['gcs_uri']}")
    else:
        print(f"\nÉchec de l'ingestion: {result['error']}")
        exit(1)
