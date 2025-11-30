import os
import sys
import pandas as pd
from google.cloud import storage
import tempfile
import logging

sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from config.settings import GCS_BUCKET_NAME, GCP_PROJECT_ID

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

GCS_PATHS = {
    "carburants": {
        "detailed": f"processed/carburants/detailed",
        "by_region": f"processed/carburants/aggregations/by_region",
        "by_departement": f"processed/carburants/aggregations/by_departement",
        "by_date": f"processed/carburants/aggregations/by_date",
        "most_expensive": f"processed/carburants/aggregations/most_expensive",
        "least_expensive": f"processed/carburants/aggregations/least_expensive",
    },
    "salaires": {
        "detailed": f"processed/salaires/detailed",
        "by_year": f"processed/salaires/aggregations/by_year",
    },
    "ipc": {
        "detailed": f"processed/ipc/detailed",
        "by_year": f"processed/ipc/aggregations/by_year",
    },
    "combined": {
        "detailed": f"processed/combined_analysis/detailed",
        "by_region": f"processed/combined_analysis/aggregations/by_region",
        "by_year": f"processed/combined_analysis/aggregations/by_year",
    }
}


def get_gcs_client():
    return storage.Client(project=GCP_PROJECT_ID)


def list_parquet_files(bucket_name: str, prefix: str) -> list:
    client = get_gcs_client()
    bucket = client.bucket(bucket_name)
    blobs = bucket.list_blobs(prefix=prefix)

    parquet_files = [f"gs://{bucket_name}/{blob.name}"
                     for blob in blobs
                     if blob.name.endswith('.parquet')]
    return parquet_files


def download_parquet_to_df(bucket_name: str, prefix: str) -> pd.DataFrame:

    client = get_gcs_client()
    bucket = client.bucket(bucket_name)
    blobs = list(bucket.list_blobs(prefix=prefix))

    parquet_blobs = [b for b in blobs if b.name.endswith('.parquet')]

    if not parquet_blobs:
        logger.warning(f"Aucun fichier Parquet trouvé dans {prefix}")
        return pd.DataFrame()

    logger.info(f"Téléchargement de {len(parquet_blobs)} fichiers Parquet depuis {prefix}...")

    dfs = []
    with tempfile.TemporaryDirectory() as tmpdir:
        for blob in parquet_blobs:
            local_path = os.path.join(tmpdir, os.path.basename(blob.name))
            blob.download_to_filename(local_path)
            df = pd.read_parquet(local_path)
            dfs.append(df)

    if dfs:
        combined_df = pd.concat(dfs, ignore_index=True)
        logger.info(f"Enregistrements chargés: {len(combined_df):,}")
        return combined_df

    return pd.DataFrame()


def load_carburants_by_region() -> pd.DataFrame:
    return download_parquet_to_df(GCS_BUCKET_NAME, GCS_PATHS["carburants"]["by_region"])


def load_carburants_by_departement() -> pd.DataFrame:
    return download_parquet_to_df(GCS_BUCKET_NAME, GCS_PATHS["carburants"]["by_departement"])


def load_carburants_by_date() -> pd.DataFrame:
    return download_parquet_to_df(GCS_BUCKET_NAME, GCS_PATHS["carburants"]["by_date"])


def load_carburants_detailed() -> pd.DataFrame:
    return download_parquet_to_df(GCS_BUCKET_NAME, GCS_PATHS["carburants"]["detailed"])


def load_carburants_top_stations() -> pd.DataFrame:
    return download_parquet_to_df(GCS_BUCKET_NAME, GCS_PATHS["carburants"]["most_expensive"])


def load_carburants_cheapest_stations() -> pd.DataFrame:
    return download_parquet_to_df(GCS_BUCKET_NAME, GCS_PATHS["carburants"]["least_expensive"])


def load_all_carburants_data() -> dict:

    logger.info("Chargement de toutes les données carburants...")

    data = {
        "by_region": load_carburants_by_region(),
        "by_departement": load_carburants_by_departement(),
        "by_date": load_carburants_by_date(),
        "most_expensive": load_carburants_top_stations(),
        "least_expensive": load_carburants_cheapest_stations(),
    }

    logger.info("Toutes les données carburants chargées")
    return data


REGION_NAMES = {
    "11": "Île-de-France",
    "24": "Centre-Val de Loire",
    "27": "Bourgogne-Franche-Comté",
    "28": "Normandie",
    "32": "Hauts-de-France",
    "44": "Grand Est",
    "52": "Pays de la Loire",
    "53": "Bretagne",
    "75": "Nouvelle-Aquitaine",
    "76": "Occitanie",
    "84": "Auvergne-Rhône-Alpes",
    "93": "Provence-Alpes-Côte d'Azur",
    "94": "Corse",
    "99": "Autres"
}

DEPARTEMENT_NAMES = {
    "75": "Paris", "13": "Bouches-du-Rhône", "69": "Rhône",
    "33": "Gironde", "59": "Nord", "31": "Haute-Garonne",
    "44": "Loire-Atlantique", "67": "Bas-Rhin", "06": "Alpes-Maritimes",
    "34": "Hérault", "83": "Var", "38": "Isère",
    "92": "Hauts-de-Seine", "93": "Seine-Saint-Denis", "94": "Val-de-Marne",
    "78": "Yvelines", "91": "Essonne", "95": "Val-d'Oise",
    "77": "Seine-et-Marne", "29": "Finistère", "35": "Ille-et-Vilaine",
    "56": "Morbihan", "22": "Côtes-d'Armor", "57": "Moselle"
}


def add_region_names(df: pd.DataFrame, region_col: str = "region_code") -> pd.DataFrame:
    if region_col in df.columns:
        df["region_name"] = df[region_col].astype(str).map(REGION_NAMES).fillna("Inconnu")
    return df


if __name__ == "__main__":
    print("Test de chargement des données...")

    try:
        df_region = load_carburants_by_region()
        print(f"\nDonnées par région: {len(df_region)} lignes")
        print(df_region.head())

        df_date = load_carburants_by_date()
        print(f"\nDonnées par date: {len(df_date)} lignes")
        print(df_date.head())

    except Exception as e:
        print(f"Erreur: {e}")
        print("Assurez-vous d'être authentifié: gcloud auth application-default login")
