from pyspark.sql import SparkSession, DataFrame
from pyspark.sql import functions as F
from pyspark.sql.types import *
from datetime import datetime
import logging

logger = logging.getLogger(__name__)


def create_spark_session(app_name: str, config: dict = None) -> SparkSession:
    builder = SparkSession.builder \
        .appName(app_name) \
        .config("spark.sql.adaptive.enabled", "true") \
        .config("spark.sql.adaptive.coalescePartitions.enabled", "true") \
        .config("spark.sql.legacy.timeParserPolicy", "LEGACY")

    if config:
        for key, value in config.items():
            builder = builder.config(key, value)

    spark = builder.getOrCreate()

    spark.sparkContext.setLogLevel("WARN")

    logger.info(f"✅ Session Spark créée: {app_name}")
    logger.info(f"   Version Spark: {spark.version}")
    logger.info(f"   Master: {spark.sparkContext.master}")

    return spark


def read_from_gcs(spark: SparkSession, gcs_path: str, file_format: str = "json") -> DataFrame:
    logger.info(f"📥 Lecture depuis GCS: {gcs_path}")

    if file_format == "json":
        df = spark.read \
            .option("multiLine", "true") \
            .option("mode", "PERMISSIVE") \
            .json(gcs_path)

        if "_corrupt_record" in df.columns and len(df.columns) == 1:
            logger.warning("⚠️  Données JSON corrompues détectées, tentative avec format JSON Lines...")
            df = spark.read \
                .option("multiLine", "false") \
                .option("mode", "PERMISSIVE") \
                .json(gcs_path)
    elif file_format == "csv":
        df = spark.read.option("header", "true").option("inferSchema", "true").csv(gcs_path)
    elif file_format == "parquet":
        df = spark.read.parquet(gcs_path)
    else:
        raise ValueError(f"Format non supporté: {file_format}")

    count = df.count()
    logger.info(f"{count} enregistrements lus")
    logger.info(f"   Colonnes: {df.columns}")

    return df


def write_to_gcs(df: DataFrame, gcs_path: str, mode: str = "overwrite",
                 file_format: str = "parquet", partition_by: list = None):
    logger.info(f"Écriture vers GCS: {gcs_path}")
    logger.info(f"   Format: {file_format}, Mode: {mode}")

    writer = df.write.mode(mode)

    if partition_by:
        writer = writer.partitionBy(*partition_by)
        logger.info(f"   Partitionnement: {partition_by}")

    if file_format == "parquet":
        writer.parquet(gcs_path)
    elif file_format == "json":
        writer.json(gcs_path)
    elif file_format == "csv":
        writer.option("header", "true").csv(gcs_path)
    else:
        raise ValueError(f"Format non supporté: {file_format}")

    logger.info(f"Données écrites avec succès")


def add_processing_metadata(df: DataFrame) -> DataFrame:
    return df.withColumn("processing_timestamp", F.lit(datetime.now().isoformat())) \
             .withColumn("processing_date", F.current_date())


def clean_column_names(df: DataFrame) -> DataFrame:
    new_columns = [col.strip().replace(' ', '_').replace('-', '_').lower()
                   for col in df.columns]

    for old_col, new_col in zip(df.columns, new_columns):
        df = df.withColumnRenamed(old_col, new_col)

    return df


def detect_outliers_iqr(df: DataFrame, column: str,
                        lower_quantile: float = 0.25,
                        upper_quantile: float = 0.75,
                        iqr_multiplier: float = 1.5) -> DataFrame:
    quantiles = df.approxQuantile(column, [lower_quantile, upper_quantile], 0.01)
    q1, q3 = quantiles[0], quantiles[1]
    iqr = q3 - q1

    lower_bound = q1 - (iqr_multiplier * iqr)
    upper_bound = q3 + (iqr_multiplier * iqr)

    logger.info(f"   Détection outliers pour {column}:")
    logger.info(f"   Q1={q1:.2f}, Q3={q3:.2f}, IQR={iqr:.2f}")
    logger.info(f"   Bornes: [{lower_bound:.2f}, {upper_bound:.2f}]")

    outlier_col_name = f"is_outlier_{column}"
    df = df.withColumn(
        outlier_col_name,
        F.when((F.col(column) < lower_bound) | (F.col(column) > upper_bound), True)
         .otherwise(False)
    )

    outlier_count = df.filter(F.col(outlier_col_name) == True).count()
    total_count = df.count()
    outlier_pct = (outlier_count / total_count) * 100 if total_count > 0 else 0

    logger.info(f"    {outlier_count} outliers détectés ({outlier_pct:.2f}%)")

    return df


def calculate_statistics(df: DataFrame, numeric_columns: list) -> dict:
    stats = {}

    for col in numeric_columns:
        col_stats = df.select(
            F.count(col).alias('count'),
            F.mean(col).alias('mean'),
            F.stddev(col).alias('stddev'),
            F.min(col).alias('min'),
            F.max(col).alias('max')
        ).collect()[0]

        stats[col] = {
            'count': col_stats['count'],
            'mean': col_stats['mean'],
            'stddev': col_stats['stddev'],
            'min': col_stats['min'],
            'max': col_stats['max']
        }

        logger.info(f"   📊 Stats {col}: "
                   f"mean={col_stats['mean']:.2f}, "
                   f"min={col_stats['min']:.2f}, "
                   f"max={col_stats['max']:.2f}")

    return stats


def show_sample(df: DataFrame, n: int = 10, description: str = "Sample"):
    logger.info(f"\n{'='*80}")
    logger.info(f"📋 {description} (showing {n} rows)")
    logger.info(f"{'='*80}")
    df.show(n, truncate=False)
    logger.info(f"{'='*80}\n")


def log_dataframe_info(df: DataFrame, name: str):
    logger.info(f"\n{'='*80}")
    logger.info(f"Informations DataFrame: {name}")
    logger.info(f"{'='*80}")
    logger.info(f"   Nombre de lignes: {df.count():,}")
    logger.info(f"   Nombre de colonnes: {len(df.columns)}")
    logger.info(f"   Colonnes: {', '.join(df.columns)}")
    logger.info(f"\n   Schéma:")
    df.printSchema()
    logger.info(f"{'='*80}\n")
