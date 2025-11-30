import json
import logging
from datetime import datetime

from data_ingestion.fetch_carburants import fetch_carburants_data, process_carburants_data, upload_to_gcs

logger = logging.getLogger(__name__)
logging.basicConfig(level=logging.INFO)


def ingest_carburants(request):
    logger.info("=" * 80)
    logger.info("DÉMARRAGE INGESTION - PRIX DES CARBURANTS (Cloud Function)")
    logger.info("=" * 80)

    try:
        # Réutiliser les fonctions existantes
        raw_data = fetch_carburants_data()
        processed_data = process_carburants_data(raw_data)
        gcs_uri = upload_to_gcs(processed_data)

        result = {
            "success": True,
            "message": "Ingestion réussie",
            "gcs_uri": gcs_uri,
            "record_count": len(processed_data),
            "timestamp": datetime.now().isoformat()
        }

        logger.info("=" * 80)
        logger.info(f"RÉSUMÉ - {len(processed_data):,} enregistrements")
        logger.info(f"Destination: {gcs_uri}")
        logger.info("=" * 80)

        return (json.dumps(result, indent=2), 200, {'Content-Type': 'application/json'})

    except Exception as e:
        error_message = f"Échec de l'ingestion: {str(e)}"
        logger.error(error_message)

        error_result = {
            "success": False,
            "message": error_message,
            "timestamp": datetime.now().isoformat()
        }

        return (json.dumps(error_result, indent=2), 500, {'Content-Type': 'application/json'})
