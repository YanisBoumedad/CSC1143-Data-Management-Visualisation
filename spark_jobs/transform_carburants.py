import sys
import logging
from pyspark.sql import functions as F
from pyspark.sql.window import Window
from pyspark.sql.types import *

from utils_spark import (
    create_spark_session,
    read_from_gcs,
    write_to_gcs,
    add_processing_metadata,
    clean_column_names,
    detect_outliers_iqr,
    log_dataframe_info,
    show_sample
)

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


def clean_carburants_data(df):
    logger.info("Nettoyage des données carburants...")

    initial_count = df.count()

    df = clean_column_names(df)

    df = df.filter(
        F.col("id").isNotNull() &
        F.col("cp").isNotNull() &
        F.col("ville").isNotNull()
    )

    df = df.withColumn("cp", F.lpad(F.col("cp").cast("string"), 5, "0"))

    if 'dep_code' in df.columns:
        df = df.withColumn("departement", F.lpad(F.col("dep_code").cast("string"), 2, "0"))
    else:
        df = df.withColumn("departement", F.substring(F.col("cp"), 1, 2))

    if 'reg_code' in df.columns:
        df = df.withColumn("region_code", F.col("reg_code").cast("string"))
    else:
        df = df.withColumn(
            "region_code",
            F.when(F.col("departement").isin(["75", "77", "78", "91", "92", "93", "94", "95"]), "11")
             .when(F.col("departement").isin(["59", "62", "02", "60", "80"]), "32")
             .when(F.col("departement").isin(["44", "49", "53", "72", "85"]), "52")
             .when(F.col("departement").isin(["21", "25", "39", "58", "70", "71", "89", "90"]), "27")
             .when(F.col("departement").isin(["67", "68", "54", "55", "57", "88"]), "44")
             .when(F.col("departement").isin(["69", "01", "07", "26", "38", "42", "73", "74"]), "84")
             .when(F.col("departement").isin(["13", "04", "05", "06", "83", "84"]), "93")
             .when(F.col("departement").isin(["31", "09", "11", "12", "30", "32", "34", "46", "48", "65", "66", "81", "82"]), "76")
             .when(F.col("departement").isin(["33", "24", "40", "47", "64", "19", "23", "87"]), "75")
             .when(F.col("departement").isin(["35", "22", "29", "56"]), "53")
             .when(F.col("departement").isin(["14", "27", "50", "61", "76"]), "28")
             .when(F.col("departement").isin(["18", "28", "36", "37", "41", "45"]), "24")
             .otherwise("99")
        )

    if 'geom' in df.columns:
        df = df.withColumn("latitude", F.col("geom.lat").cast("double"))
        df = df.withColumn("longitude", F.col("geom.lon").cast("double"))
        logger.info("   Coordonnées GPS extraites (geom.lat → latitude, geom.lon → longitude)")
    else:
        logger.warning("   Colonne 'geom' introuvable, coordonnées GPS non extraites")

    if 'prix_valeur' in df.columns:
        df = df.withColumn("prix_valeur", F.col("prix_valeur").cast("double"))
        df = df.withColumn("prix_valeur", F.when((F.col("prix_valeur") > 0) & (F.col("prix_valeur") < 5.0), F.col("prix_valeur")).otherwise(None))

    if 'prix_maj' in df.columns:
        df = df.withColumn("prix_maj_date", F.to_date(F.col("prix_maj")))
        df = df.withColumn("annee", F.year(F.col("prix_maj_date")))

    final_count = df.count()
    removed = initial_count - final_count
    logger.info(f" Nettoyage terminé: {removed:,} enregistrements supprimés ({(removed/initial_count*100):.1f}%)")
    logger.info(f"   Enregistrements restants: {final_count:,}")

    return df


def pivot_carburants_data(df):
    logger.info("Transformation pivot des données...")

    initial_count = df.count()

    group_columns = [col for col in df.columns
                     if col not in ['prix_nom', 'prix_valeur', 'prix_maj', 'prix_id', 'ingestion_timestamp']]

    logger.info(f"   Colonnes de groupement: {group_columns}")

    df_pivoted = df.groupBy(group_columns).pivot("prix_nom").agg(F.first("prix_valeur"))

    for col in df_pivoted.columns:
        if col not in group_columns:
            new_col = col.lower().replace(" ", "_").replace("-", "_")
            df_pivoted = df_pivoted.withColumnRenamed(col, new_col)

    final_count = df_pivoted.count()

    prix_cols = [col for col in df_pivoted.columns if col not in group_columns]
    logger.info(f" Pivot terminé: {initial_count:,} lignes → {final_count:,} stations")
    logger.info(f"   Colonnes de prix créées: {prix_cols}")

    return df_pivoted


def create_features(df):
    logger.info("  Feature engineering...")

    prix_columns = [col for col in df.columns if col in ['gazole', 'sp95', 'sp98', 'e10', 'e85', 'gplc']]
    logger.info(f"   Colonnes de prix détectées: {prix_columns}")

    valid_prix_cols = [F.col(c) for c in prix_columns if c in df.columns]
    if valid_prix_cols:
        df = df.withColumn("prix_moyen_station",
                          F.round((sum(valid_prix_cols) / len(valid_prix_cols)), 3))

    window_region = Window.partitionBy("region_code")

    for prix_col in prix_columns:
        if prix_col in df.columns:
            avg_col_name = f"{prix_col}_moy_region"
            diff_col_name = f"{prix_col}_diff_region"
            pct_col_name = f"{prix_col}_pct_vs_region"

            df = df.withColumn(avg_col_name, F.round(F.avg(F.col(prix_col)).over(window_region), 3))

            df = df.withColumn(diff_col_name,
                             F.round(F.col(prix_col) - F.col(avg_col_name), 3))

            df = df.withColumn(pct_col_name,
                             F.round(((F.col(prix_col) - F.col(avg_col_name)) / F.col(avg_col_name)) * 100, 2))

    if 'gazole' in df.columns:
        df = df.withColumn(
            "competitive_gazole",
            F.when(F.col("gazole_diff_region") < 0, True).otherwise(False)
        )

    if 'sp95' in df.columns:
        df = df.withColumn(
            "competitive_sp95",
            F.when(F.col("sp95_diff_region") < 0, True).otherwise(False)
        )

    competitive_cols = [F.when(F.col(f"{prix_col}_diff_region") < 0, 1).otherwise(0)
                       for prix_col in prix_columns if f"{prix_col}_diff_region" in df.columns]

    if competitive_cols:
        df = df.withColumn("competitivity_score", sum(competitive_cols))

    if 'prix_maj_date' in df.columns and 'gazole' in df.columns:
        window_station = Window.partitionBy("id").orderBy("prix_maj_date")
        df = df.withColumn("gazole_evolution",
                          F.col("gazole") - F.lag("gazole", 1).over(window_station))

    logger.info(" Feature engineering terminé")

    return df


def detect_anomalies(df):
    logger.info(" Détection d'anomalies...")

    prix_columns = [col for col in df.columns if col in ['gazole', 'sp95', 'sp98', 'e10', 'e85', 'gplc']]

    for prix_col in prix_columns:
        if prix_col in df.columns:
            logger.info(f"   Analyse des anomalies pour: {prix_col}")
            df = detect_outliers_iqr(df, prix_col, iqr_multiplier=1.5)

    outlier_cols = [c for c in df.columns if c.startswith('is_outlier_')]
    if outlier_cols:
        df = df.withColumn(
            "has_any_anomaly",
            F.array_contains(F.array([F.col(c) for c in outlier_cols]), True)
        )

        anomaly_count = df.filter(F.col("has_any_anomaly") == True).count()
        total = df.count()
        logger.info(f"      {anomaly_count:,} stations avec au moins une anomalie ({(anomaly_count/total*100):.2f}%)")

    logger.info(" Détection d'anomalies terminée")

    return df


def create_aggregations(df):
    logger.info(" Création des agrégations...")

    aggregations = {}

    prix_columns = [col for col in df.columns if col in ['gazole', 'sp95', 'sp98', 'e10', 'e85', 'gplc']]

    agg_expressions = []
    for col in prix_columns:
        if col in df.columns:
            agg_expressions.extend([
                F.avg(col).alias(f"{col}_moyen"),
                F.min(col).alias(f"{col}_min"),
                F.max(col).alias(f"{col}_max"),
                F.stddev(col).alias(f"{col}_stddev")
            ])

    agg_expressions.append(F.count("*").alias("nombre_stations"))

    aggregations['by_region'] = df.groupBy("region_code").agg(*agg_expressions)
    logger.info(f"   Agrégation par région: {aggregations['by_region'].count()} régions")

    aggregations['by_departement'] = df.groupBy("departement", "region_code").agg(*agg_expressions)
    logger.info(f"   Agrégation par département: {aggregations['by_departement'].count()} départements")

    if 'prix_maj_date' in df.columns:
        temporal_aggs = [F.avg(col).alias(f"{col}_moyen") for col in prix_columns if col in df.columns]
        temporal_aggs.append(F.count("*").alias("nombre_stations"))

        aggregations['by_date'] = df.groupBy("prix_maj_date").agg(*temporal_aggs)
        logger.info(f"   Agrégation temporelle: {aggregations['by_date'].count()} dates")

        aggregations['by_departement_date'] = df.groupBy("departement", "region_code", "prix_maj_date") \
            .agg(*temporal_aggs) \
            .withColumnRenamed("prix_maj_date", "date")
        logger.info(f"   Agrégation département × date: {aggregations['by_departement_date'].count()} lignes")

    if 'annee' in df.columns:
        yearly_aggs = [F.avg(col).alias(f"{col}_moyen") for col in prix_columns if col in df.columns]
        yearly_aggs.append(F.count("*").alias("nombre_stations"))

        aggregations['by_year'] = df.groupBy("annee").agg(*yearly_aggs).orderBy("annee")
        logger.info(f"   Agrégation par année: {aggregations['by_year'].count()} années")

        aggregations['by_departement_year'] = df.groupBy("departement", "region_code", "annee") \
            .agg(*yearly_aggs) \
            .orderBy("departement", "annee")
        logger.info(f"   Agrégation département × année: {aggregations['by_departement_year'].count()} lignes")

    if 'gazole' in df.columns:
        window_top = Window.partitionBy("region_code").orderBy(F.desc("gazole"))
        aggregations['most_expensive'] = df.withColumn("rank", F.row_number().over(window_top)) \
                                          .filter(F.col("rank") <= 10) \
                                          .select("id", "ville", "departement", "region_code", "gazole", "rank")

        window_bottom = Window.partitionBy("region_code").orderBy(F.asc("gazole"))
        aggregations['least_expensive'] = df.withColumn("rank", F.row_number().over(window_bottom)) \
                                           .filter(F.col("rank") <= 10) \
                                           .select("id", "ville", "departement", "region_code", "gazole", "rank")

        logger.info("   Top/Bottom stations par région")

    logger.info("Agrégations terminées")

    return aggregations


def main():
    logger.info("="*80)
    logger.info(" DÉMARRAGE TRAITEMENT SPARK - PRIX DES CARBURANTS")
    logger.info("="*80)

    GCS_BUCKET = "csc1142-projet"
    INPUT_PATH = f"gs://{GCS_BUCKET}/raw/carburants/*.json"
    OUTPUT_BASE = f"gs://{GCS_BUCKET}/processed/carburants"

    try:
        spark = create_spark_session(
            "Transform_Carburants",
            config={
                "spark.sql.shuffle.partitions": "200",
                "spark.default.parallelism": "200"
            }
        )

        df_raw = read_from_gcs(spark, INPUT_PATH, file_format="json")
        log_dataframe_info(df_raw, "Données brutes")

        df_clean = clean_carburants_data(df_raw)
        log_dataframe_info(df_clean, "Données nettoyées")

        df_pivoted = pivot_carburants_data(df_clean)
        log_dataframe_info(df_pivoted, "Après pivot")

        df_features = create_features(df_pivoted)
        log_dataframe_info(df_features, "Avec features")

        df_final = detect_anomalies(df_features)
        df_final = add_processing_metadata(df_final)

        show_sample(df_final.select("id", "ville", "region_code", "gazole", "gazole_moy_region",
                                    "gazole_diff_region", "competitive_gazole", "has_any_anomaly"),
                   n=20, description="Résultats finaux")

        write_to_gcs(
            df_final,
            f"{OUTPUT_BASE}/detailed",
            mode="overwrite",
            file_format="parquet",
            partition_by=["region_code", "departement"]
        )

        aggregations = create_aggregations(df_final)

        for agg_name, agg_df in aggregations.items():
            write_to_gcs(
                agg_df,
                f"{OUTPUT_BASE}/aggregations/{agg_name}",
                mode="overwrite",
                file_format="parquet"
            )
            logger.info(f"   Agrégation '{agg_name}' sauvegardée")

        logger.info("\n" + "="*80)
        logger.info("STATISTIQUES FINALES")
        logger.info("="*80)
        logger.info(f"   Total stations traitées: {df_final.count():,}")
        logger.info(f"   Nombre de régions: {df_final.select('region_code').distinct().count()}")
        logger.info(f"   Nombre de départements: {df_final.select('departement').distinct().count()}")

        if 'annee' in df_final.columns:
            years = df_final.select('annee').distinct().orderBy('annee').collect()
            year_list = [row['annee'] for row in years if row['annee'] is not None]
            if year_list:
                logger.info(f"   Période couverte: {min(year_list)} - {max(year_list)} ({len(year_list)} années)")

        if 'has_any_anomaly' in df_final.columns:
            anomalies = df_final.filter(F.col("has_any_anomaly") == True).count()
            logger.info(f"   Stations avec anomalies: {anomalies:,}")

        logger.info("="*80)
        logger.info("TRAITEMENT TERMINÉ AVEC SUCCÈS")
        logger.info("="*80)

        spark.stop()
        return 0

    except Exception as e:
        logger.error(f"ERREUR lors du traitement: {str(e)}", exc_info=True)
        return 1


if __name__ == "__main__":
    sys.exit(main())
