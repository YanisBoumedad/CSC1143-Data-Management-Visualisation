# Spark Pipeline - Fuel Price Processing

This folder contains the Spark pipeline for processing and analyzing fuel price data in France.

## Overview

The pipeline transforms raw JSON fuel price data into optimized Parquet datasets with multiple aggregations to facilitate analysis and visualization.

### Data Flow

```
Raw JSON (API)
    ↓
[Ingestion] → gs://csc1142-projet/raw/carburants/
    ↓
[Spark Transform] → Cleaning + Enrichment + Aggregations
    ↓
[Parquet] → gs://csc1142-projet/processed/carburants/
    ↓
[Visualization] → Streamlit Dashboard
```

## Pipeline Architecture

### 1. Data Source

**Government API**: Real-time fuel prices
- Format: JSON
- Frequency: Daily updates
- Structure: Vertical format (1 row per station × fuel × date)

Example of raw JSON structure:
```json
{
  "id": "59000001",
  "cp": "59000",
  "ville": "Lille",
  "adresse": "123 Rue de la République",
  "geom": {"lat": 50.6292, "lon": 3.0573},
  "prix_nom": "Gazole",
  "prix_valeur": 1.789,
  "prix_maj": "2025-11-27T10:00:00",
  "prix_id": "1234"
}
```

### 2. Spark Job: transform_carburants.py

The main job performs 6 transformation steps:

#### Step 1: Data Cleaning

**Function**: `clean_carburants_data(df)`

**Transformations**:
- Column name normalization (lowercase, underscore)
- Postal code formatting (padding to 5 digits: "75" → "75000")
- GPS coordinates extraction from `geom.lat` / `geom.lon` structure
- Department calculation from postal code (first 2 digits)
- Department → region mapping (INSEE correspondence rules)
- Price validation (filtering aberrant values < 0 or > 5 €/L)
- Date conversion to timestamp format

**Example code**:
```python
df = df.withColumn("cp", F.lpad(F.col("cp").cast("string"), 5, "0"))
df = df.withColumn("departement", F.substring(F.col("cp"), 1, 2))
df = df.withColumn("latitude", F.col("geom.lat").cast("double"))
df = df.withColumn("longitude", F.col("geom.lon").cast("double"))
```

**Region mapping**:
- Île-de-France (11): 75, 77, 78, 91, 92, 93, 94, 95
- Hauts-de-France (32): 59, 62, 02, 60, 80
- Pays de la Loire (52): 44, 49, 53, 72, 85
- Etc. (13 regions total)

#### Step 2: Data Pivoting

**Function**: `pivot_carburants_data(df)`

**Transformation**: Vertical format → Horizontal format

Before:
```
id        | prix_nom | prix_valeur | ville
59000001  | Gazole   | 1.789      | Lille
59000001  | SP95     | 1.899      | Lille
59000001  | E10      | 1.859      | Lille
```

After:
```
id        | gazole | sp95  | e10   | ville
59000001  | 1.789  | 1.899 | 1.859 | Lille
```

**Fuel types processed**:
- Gazole (diesel)
- SP95 (unleaded 95)
- SP98 (unleaded 98)
- E10 (10% ethanol)
- E85 (superethanol 85% ethanol)
- GPLc (LPG fuel)

#### Step 3: Feature Engineering

**Function**: `create_features(df)`

**Calculated features**:

1. **Average price per station**
```python
prix_moyen_station = (gazole + sp95 + sp98 + e10) / 4
```

2. **Comparison with regional average** (Window functions)
```python
window_region = Window.partitionBy("region_code")
gazole_moy_region = AVG(gazole) OVER window_region
gazole_diff_region = gazole - gazole_moy_region
gazole_pct_vs_region = (gazole_diff_region / gazole_moy_region) * 100
```

3. **Competitiveness indicator**
```python
competitive_gazole = (gazole < gazole_moy_region)
competitivity_score = COUNT(prix < prix_moy_region) per station
```

4. **Time evolution** (Lag functions)
```python
window_station = Window.partitionBy("id").orderBy("prix_maj_date")
gazole_evolution = gazole - LAG(gazole, 1) OVER window_station
```

#### Step 4: Anomaly Detection

**Function**: `detect_anomalies(df)`

**Method**: IQR (Interquartile Range)

```python
Q1 = percentile_25(prix)
Q3 = percentile_75(prix)
IQR = Q3 - Q1
lower_bound = Q1 - 1.5 × IQR
upper_bound = Q3 + 1.5 × IQR

is_outlier = (prix < lower_bound) OR (prix > upper_bound)
```

**Result**:
- Creation of an `is_outlier_{fuel}` column for each fuel
- `has_any_anomaly` column = True if at least one abnormal price

#### Step 5: Aggregation Creation

**Function**: `create_aggregations(df)`

**Produced aggregations**:

1. **by_region**: Averages by region
```sql
SELECT
    region_code,
    AVG(gazole) as gazole_moyen,
    MIN(gazole) as gazole_min,
    MAX(gazole) as gazole_max,
    STDDEV(gazole) as gazole_stddev,
    COUNT(*) as nombre_stations
FROM carburants
GROUP BY region_code
```

2. **by_departement**: Statistics by department
```sql
SELECT
    departement,
    region_code,
    AVG(gazole) as gazole_moyen,
    MIN(gazole) as gazole_min,
    MAX(gazole) as gazole_max,
    STDDEV(gazole) as gazole_stddev,
    COUNT(*) as nombre_stations
FROM carburants
GROUP BY departement, region_code
```

3. **by_date**: Daily time evolution
```sql
SELECT
    prix_maj_date,
    AVG(gazole) as gazole_moyen,
    AVG(sp95) as sp95_moyen,
    AVG(e10) as e10_moyen,
    COUNT(*) as nombre_stations
FROM carburants
GROUP BY prix_maj_date
ORDER BY prix_maj_date
```

4. **by_year**: Annual averages
```sql
SELECT
    YEAR(prix_maj_date) as annee,
    AVG(gazole) as gazole_moyen,
    COUNT(*) as nombre_stations
FROM carburants
GROUP BY YEAR(prix_maj_date)
ORDER BY annee
```

5. **by_departement_date**: Time series by department
```sql
SELECT
    departement,
    region_code,
    prix_maj_date as date,
    AVG(gazole) as gazole_moyen,
    COUNT(*) as nombre_stations
FROM carburants
GROUP BY departement, region_code, prix_maj_date
```

6. **most_expensive** / **least_expensive**: Extreme stations
```sql
-- Top 10 per region
SELECT id, ville, departement, region_code, gazole,
       ROW_NUMBER() OVER (PARTITION BY region_code ORDER BY gazole DESC) as rank
FROM carburants
WHERE rank <= 10
```

#### Step 6: Parquet Storage

**Format**: Apache Parquet with Snappy compression
**Partitioning**: By `region_code` and `departement` for detailed data

```python
df.write \
    .mode("overwrite") \
    .partitionBy("region_code", "departement") \
    .parquet("gs://csc1142-projet/processed/carburants/detailed/")
```

## Processed Data Statistics

### Source data
- **Period**: September 2007 - November 2025 (18 years)
- **Number of dates**: 279 dates with data
- **Unique stations**: ~48,000 gas stations in France

### Pipeline results

#### Detailed data
- **Format**: Partitioned Parquet
- **Size**: ~50,000+ records
- **Columns**: 50+ columns (6 fuels × statistics + metadata)

#### Aggregations

| Aggregation | Number of rows | Description |
|------------|------------------|-------------|
| by_region | 13 regions | National averages by region |
| by_departement | 97 departments | Department statistics |
| by_date | 279 dates | Daily time evolution |
| by_year | 11 years | Annual averages (2007-2025) |
| by_departement_date | ~27,000 | Department time series |
| by_departement_year | ~1,000 | Annual evolution by department |
| most_expensive | 140 stations | Top expensive stations (10 per region) |
| least_expensive | 140 stations | Top economical stations |

### Diesel Prices (real statistics)

```
Analysis period: 2014 - 2025

Minimum price: 1.530 €/L (01/09/2014)
Maximum price: 2.200 €/L (01/10/2024)  [Energy crisis peak]
Current price: 1.708 €/L (24/11/2025)

Total evolution: +0.178 €/L (+11.6% since 2014)

Recent annual variations:
  2022: 2.060 €/L  (+34.6% vs 2014)
  2023: 1.950 €/L  (-5.3% vs 2022)
  2024: 1.878 €/L  (-3.7% vs 2023)
  2025: 1.708 €/L  (-9.1% vs 2024)
```

### Regional disparities

```
Cheapest region: Brittany (53)        - 1.685 €/L
Most expensive region: Corsica (94)   - 1.808 €/L
Regional gap: 0.123 €/L (7.3%)

Top 3 economical regions:
  1. Brittany (53)           : 1.685 €/L
  2. Pays de la Loire (52)   : 1.702 €/L
  3. Nouvelle-Aquitaine (75) : 1.704 €/L

Top 3 expensive regions:
  1. Corsica (94)              : 1.808 €/L
  2. Île-de-France (11)        : 1.769 €/L
  3. Auvergne-Rhône-Alpes (84) : 1.726 €/L
```

## Spark Optimizations

### Applied configuration

```python
spark = SparkSession.builder \
    .appName("Transform_Carburants") \
    .config("spark.sql.adaptive.enabled", "true") \
    .config("spark.sql.adaptive.coalescePartitions.enabled", "true") \
    .config("spark.sql.shuffle.partitions", "200") \
    .config("spark.default.parallelism", "200") \
    .getOrCreate()
```

### Optimization techniques

1. **Adaptive Query Execution (AQE)**
   - Dynamic optimization of execution plans
   - Automatic adjustment of partition numbers
   - Dynamic conversion of joins to broadcast joins

2. **Intelligent partitioning**
   - Data partitioned by `region_code` and `departement`
   - Allows very fast filters on these columns
   - 90%+ data scan reduction

3. **DataFrame caching**
   - Pivoted DataFrame cached for multiple aggregations
   - Avoids recalculating pivot for each aggregation

4. **Broadcast Joins**
   - Reference tables (region codes) broadcasted
   - Avoids expensive shuffles

5. **Parquet format**
   - Snappy columnar compression
   - 70-80% size reduction vs JSON
   - Ultra-fast reading of specific columns

### Performance

```
Data volume:
  Raw JSON      : ~500 MB
  Total Parquet : ~120 MB (76% compression)

Execution time (on 3-node cluster):
  JSON reading        : ~15s
  Cleaning + Pivot    : ~30s
  Feature Engineering : ~20s
  Aggregations (×8)   : ~40s
  Parquet writing     : ~25s
  -------------------------
  Total               : ~2min 10s

Parquet queries:
  Filter 1 region     : <1s
  Filter 1 department : <1s
  Full scan           : ~3s
```

## File Structure

```
spark_jobs/
├── README.md                      # This file
├── transform_carburants.py        # Main transformation job
├── utils_spark.py                 # Spark utility functions
```

## Usage

### Prerequisites

```bash
# Python 3.9+
pip install pyspark==3.5.0 pandas pyarrow

# GCP Authentication
gcloud auth application-default login
```



## Data Schemas

### Detailed data (detailed/)

| Column | Type | Description |
|---------|------|-------------|
| id | string | Unique station identifier |
| ville | string | City |
| adresse | string | Full address |
| cp | string | Postal code (5 digits) |
| departement | string | Department code (2 digits) |
| region_code | string | INSEE region code |
| latitude | double | GPS coordinate |
| longitude | double | GPS coordinate |
| gazole | double | Diesel price (€/L) |
| sp95 | double | SP95 price (€/L) |
| sp98 | double | SP98 price (€/L) |
| e10 | double | E10 price (€/L) |
| e85 | double | E85 price (€/L) |
| gplc | double | LPG price (€/L) |
| prix_maj_date | timestamp | Update date |
| prix_moyen_station | double | Station average price |
| gazole_moy_region | double | Regional diesel average |
| gazole_diff_region | double | Difference vs regional average |
| gazole_pct_vs_region | double | Difference in % vs region |
| competitive_gazole | boolean | Price < regional average |
| competitivity_score | int | Number of competitive fuels |
| gazole_evolution | double | Variation vs last update |
| is_outlier_gazole | boolean | Detected anomaly (IQR) |
| has_any_anomaly | boolean | At least one anomaly |
| processing_timestamp | string | Processing date |
| processing_date | date | Processing date |

### Aggregations (aggregations/*)

**by_region**, **by_departement**:
- {fuel}_moyen: Average price
- {fuel}_min: Minimum price
- {fuel}_max: Maximum price
- {fuel}_stddev: Standard deviation
- nombre_stations: Number of stations

**by_date**, **by_year**:
- {fuel}_moyen: Average price
- nombre_stations: Number of observed stations

## Result Visualization

Transformed data can be visualized with:

### 1. Python visualization scripts

```bash
# Interactive menu
python parquet_viewers/view_all.py

# Or specific scripts
python parquet_viewers/view_by_region.py
python parquet_viewers/view_by_date.py
```

### 2. Streamlit Dashboard

```bash
streamlit run visualization/app_visualisation.py
```

### 3. Direct queries with Pandas

```python
import pandas as pd

# Load an aggregation
df = pd.read_parquet("gs://csc1142-projet/processed/carburants/aggregations/by_region/")
print(df.head())

# Analyze
print(f"National average price: {df['gazole_moyen'].mean():.3f} €/L")
```

## Monitoring and Logs

The job produces detailed logs:

```
================================================================================
STARTING SPARK PROCESSING - FUEL PRICES
================================================================================
Reading from GCS: gs://csc1142-projet/raw/carburants/*.json
50913 records read

Cleaning fuel price data...
  Cleaning completed: 145 records removed (0.3%)
  Remaining records: 50,768

Pivoting data transformation...
  Pivot completed: 50,913 rows → 48,793 stations

Feature engineering...
  Feature engineering completed

Detecting anomalies...
  Anomaly analysis for: gazole
    123 stations with at least one anomaly (0.25%)

Creating aggregations...
  Aggregation by region: 13 regions
  Aggregation by department: 97 departments
  Time aggregation: 279 dates
  Top/Bottom stations per region

FINAL STATISTICS
  Total stations processed: 48,793
  Number of regions: 13
  Number of departments: 97
  Period covered: 2007 - 2025 (11 years)
  Stations with anomalies: 123

PROCESSING COMPLETED SUCCESSFULLY
```

## Maintenance

### Restart the pipeline

```bash
# Delete processed data
gsutil -m rm -r gs://csc1142-projet/processed/carburants/*

# Restart processing
python spark_jobs/transform_carburants.py
```

### Add a new aggregation

1. Modify `create_aggregations()` in [transform_carburants.py](transform_carburants.py)
2. Add aggregation logic
3. Restart the job

Example:
```python
# In create_aggregations()
aggregations['by_city'] = df.groupBy("ville", "departement").agg(
    F.avg("gazole").alias("gazole_moyen"),
    F.count("*").alias("nombre_stations")
)
```

## Dependencies

```
pyspark==3.5.0
pandas==2.1.4
pyarrow==14.0.1
google-cloud-storage==2.14.0
```

## Troubleshooting

### Error: "File not found"
```bash
# Check raw data
gsutil ls gs://csc1142-projet/raw/carburants/
```

### Error: "Permission denied"
```bash
# Re-authenticate
gcloud auth application-default login
```

### Slow job
```bash
# Increase Dataproc resources
gcloud dataproc clusters update csc1142-spark-cluster \
  --region=europe-west1 \
  --num-workers=4
```

## Additional Documentation

- [Parquet Viewers Guide](../parquet_viewers/README.md)
- [Streamlit Dashboard Guide](../visualization/README.md)
- [Main Project README](../README.md)

## Author

Academic project - DCU Cloud Technologies
