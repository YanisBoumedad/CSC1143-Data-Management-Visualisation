# Cloud Data Pipeline - French Fuel Price Analysis

Data processing and analysis pipeline for fuel prices in France using Apache Spark on Google Cloud Platform. The project transforms raw JSON data into optimized Parquet datasets with multiple aggregations for analysis and visualization.

## Overview

This project implements a complete ETL pipeline to analyze fuel price trends in France over **18 years** (2007-2025), covering **~50,000 gas stations** and **13 regions**.

### Key Statistics

```
Analysis Period: September 2007 - November 2025 (18 years)
Data Points: 279 dates
Regions: 13 French regions
Departments: 97 departments
Stations: ~48,000 gas stations

Diesel Prices (2025):
  Current: 1.708 €/L
  Minimum: 1.530 €/L (2014)
  Maximum: 2.200 €/L (2024, energy crisis peak)
  Evolution since 2014: +11.6%

Regional Disparities:
  Cheapest Region: Brittany (1.685 €/L)
  Most Expensive Region: Corsica (1.808 €/L)
  Regional Gap: 0.123 €/L (7.3%)
```

## Architecture

```
┌──────────────────┐
│  Public API      │  Fuel prices (data.gouv.fr)
│  Government      │
└────────┬─────────┘
         │ HTTP GET
         ▼
┌──────────────────┐
│ Data Ingestion   │  Python scripts (fetch_carburants.py)
│ (data_ingestion/)│  - JSON retrieval
└────────┬─────────┘  - Timestamp enrichment
         │ Upload
         ▼
┌──────────────────┐
│ GCS (raw/)       │  gs://csc1142-projet/raw/carburants/
│ Raw Storage      │  Format: JSON (~500 MB)
└────────┬─────────┘
         │ Read
         ▼
┌──────────────────┐
│ Spark Processing │  PySpark jobs (transform_carburants.py)
│ (spark_jobs/)    │  - Cleaning + Validation
│                  │  - Pivot vertical → horizontal
└────────┬─────────┘  - Feature engineering (window functions)
         │            - Anomaly detection (IQR)
         │            - Multiple aggregations (×8)
         │ Write
         ▼
┌──────────────────┐
│ GCS (processed/) │  gs://csc1142-projet/processed/carburants/
│ Optimized Parquet│  - detailed/ (partitioned by region/dept)
└────────┬─────────┘  - aggregations/ (8 types)
         │            Format: Parquet Snappy (~120 MB, -76%)
         │ Read
         ▼
┌──────────────────┐
│ Visualization    │  - Interactive Streamlit dashboard
│ (visualization/) │  - Choropleth map by department
└──────────────────┘  - Time series charts
                      - Regional comparative analysis
```

## Main Components

### 1. Data Ingestion (`data_ingestion/`)

**Role**: Daily data retrieval from government API

**Scripts**:
- `fetch_carburants.py`: Fuel price ingestion
- `utils.py`: GCS upload functions and metadata management

**Features**:
- JSON retrieval from public API
- Enrichment with ingestion timestamps
- Postal code normalization (padding to 5 digits)
- Automatic upload to GCS
- Metadata generation (size, record count, checksums)

**Output**:
```
gs://csc1142-projet/raw/carburants/
└── carburants_YYYYMMDD_HHMMSS.json
```

### 2. Cloud Functions (`cloud_functions/`)

**Role**: Serverless automatic data ingestion with scheduled execution

**Architecture**:
```
Cloud Scheduler (daily cron at 2am)
         ↓ HTTP POST
Cloud Function (ingest-carburants-daily)
         ↓ fetch
Government API (data.gouv.fr)
         ↓ process + enrich
Google Cloud Storage (gs://csc1142-projet/raw/carburants/)
```

**Key Features**:
- **Serverless**: No server infrastructure to manage
- **Automated**: Daily execution via Cloud Scheduler (2:00 AM Paris time)
- **Cost-effective**: Pay-per-use model (~€0.82/month)
- **Scalable**: Automatic scaling based on demand
- **Resilient**: Built-in retry mechanism on failure

**Components**:
- `data_ingestion_function/main.py`: Cloud Function entry point
- `deploy_all.sh`: Automated deployment script
- Cloud Scheduler job: `ingest-carburants-scheduler`

**Deployment**:
```bash
# From project root
./cloud_functions/deploy_all.sh
```

**Monitoring**:
```bash
# View logs
gcloud functions logs read ingest-carburants-daily \
  --region=europe-west1 --gen2 --limit=50

# Test manually
gcloud scheduler jobs run ingest-carburants-scheduler \
  --location=europe-west1
```

**Configuration**:
- Runtime: Python 3.11
- Memory: 2GB
- Timeout: 540s (9 minutes)
- Region: europe-west1
- Service Account: `data-ingestion-sa@regal-sun-478114-q5.iam.gserviceaccount.com`

For detailed documentation, see [Cloud Functions README](cloud_functions/README.md).

### 3. Spark Processing (`spark_jobs/`)

**Role**: Complete ETL transformation from raw data to optimized datasets

**Files**:
- `transform_carburants.py`: Main transformation job (6 steps)
- `utils_spark.py`: Reusable Spark functions library

**Transformation Pipeline** (6 steps):

#### Step 1: Cleaning
- Column name normalization (lowercase, underscores)
- Price validation (0 < price < 5 €/L)
- GPS coordinates extraction (`geom.lat` → `latitude`, `geom.lon` → `longitude`)
- Department calculation (first 2 digits of postal code)
- Department → region mapping (13 INSEE regions)
- Date conversion to timestamps

#### Step 2: Data Pivoting
Vertical → horizontal format:
```
Before (vertical):          After (horizontal):
id | prix_nom | prix        id | gazole | sp95 | sp98 | e10
1  | Gazole   | 1.789   →   1  | 1.789  | 1.899 | 1.999 | 1.859
1  | SP95     | 1.899
1  | SP98     | 1.999
1  | E10      | 1.859
```

6 fuel types processed: Gazole (Diesel), SP95, SP98, E10, E85, GPLc

#### Step 3: Feature Engineering
- Average price per station
- Comparison vs regional average (Window functions)
- Absolute and percentage difference vs region
- Competitiveness indicator (price < regional average)
- Competitiveness score (number of competitive fuels)
- Time evolution (Lag functions)

#### Step 4: Anomaly Detection
- IQR method (Interquartile Range, multiplier 1.5)
- `is_outlier_{fuel}` columns per fuel type
- `has_any_anomaly` column (boolean aggregation)
- ~0.25% of stations with detected anomalies

#### Step 5: Aggregations
Creation of 8 aggregation types:
1. **by_region**: 13 regions (averages, min, max, stddev)
2. **by_departement**: 97 departments
3. **by_date**: 279 dates (daily evolution)
4. **by_year**: 11 years (2007-2025)
5. **by_departement_date**: ~27,000 rows (time series)
6. **by_departement_year**: ~1,000 rows
7. **most_expensive**: 140 stations (top 10 per region)
8. **least_expensive**: 140 stations (bottom 10 per region)

#### Step 6: Parquet Storage
- Apache Parquet format with Snappy compression
- Partitioning by `region_code` and `departement`
- 76% size reduction (500 MB → 120 MB)

**Output**:
```
gs://csc1142-projet/processed/carburants/
├── detailed/                           # 50,000+ records
│   ├── region_code=11/
│   │   ├── departement=75/
│   │   └── departement=77/
│   └── ...
└── aggregations/
    ├── by_region/                      # 13 rows
    ├── by_departement/                 # 97 rows
    ├── by_date/                        # 279 rows
    ├── by_year/                        # 11 rows
    ├── by_departement_date/            # ~27,000 rows
    ├── by_departement_year/            # ~1,000 rows
    ├── most_expensive/                 # 140 rows
    └── least_expensive/                # 140 rows
```

**Spark Optimizations**:
- Adaptive Query Execution (AQE)
- Intelligent partitioning for fast filtering
- DataFrame caching for multiple aggregations
- Broadcast Joins for reference tables

### 4. Visualization Dashboard (`visualization/`)

**Role**: Interactive web application for fuel price analysis and exploration

**Technology**: Streamlit with Plotly and Folium

**Key Features**:
- **Real-time data loading**: Direct access to processed Parquet files from GCS
- **Interactive visualizations**: Dynamic charts with filtering and zoom capabilities
- **Multiple views**:
  - Overview dashboard with key metrics
  - Temporal evolution analysis with customizable fuel selection
  - Regional comparison maps with color-coded pricing
  - Interactive radar charts for multi-region comparison
  - Top/Bottom stations ranking
  - Detailed statistics with distribution analysis

**Main Components**:
- `dashboard.py`: Main Streamlit application with multi-page navigation
- `charts.py`: Plotly chart creation functions
- `maps.py`: Interactive map visualizations with Folium
- `load_data.py`: Data loading utilities with caching
- `carte_prix_carburants.py`: Standalone choropleth map generator

**Launch Dashboard**:
```bash
cd visualization
streamlit run dashboard.py
```

The dashboard opens automatically at `http://localhost:8501` with:
- Sidebar navigation for different analysis views
- Cached data loading for optimal performance
- Responsive layout adapting to screen size
- Export functionality for CSV downloads

**Available Views**:
1. **Vue d'ensemble**: Summary metrics and quick insights
2. **Évolution temporelle**: Time series analysis with multi-fuel selection
3. **Carte régionale**: Regional price maps and bar charts
4. **Comparaisons**: Radar charts and deviation from national average
5. **Top/Bottom stations**: Rankings of most/least expensive stations
6. **Statistiques**: Detailed statistics, distributions, and data export



## Installation and Configuration

### Prerequisites

```
Python 3.9+
Apache Spark 3.5.0
Google Cloud SDK
GCP Account with:
  - Cloud Storage API enabled
  - Dataproc API enabled
  - Service Account with Storage Admin permissions
```

### 1. Clone the repository

```bash
git clone <repository-url>
cd Cloud-Technologies
```

### 2. Install dependencies

```bash
pip install -r requirements.txt
```

**Main dependencies**:
```
pyspark==3.5.0
pandas==2.1.4
pyarrow==14.0.1
google-cloud-storage==2.14.0
streamlit==1.29.0
plotly==5.18.0
folium>=0.15.0
requests==2.31.0
```

### 3. GCP Configuration

**Create configuration file** `config/settings.py`:

```python
GCP_PROJECT_ID = "regal-sun-478114-q5"
GCS_BUCKET_NAME = "csc1142-projet"
GCS_RAW_PATH = "raw"
GCS_PROCESSED_PATH = "processed"

API_URLS = {
    "carburants": "https://data.economie.gouv.fr/api/explore/v2.1/catalog/datasets/prix-des-carburants-en-france-flux-instantane-v2/exports/json"
}
```

**GCP Authentication**:

```bash
gcloud auth login
gcloud auth application-default login
gcloud config set project regal-sun-478114-q5
```

### 4. Create GCS bucket

```bash
gsutil mb -p regal-sun-478114-q5 -c STANDARD -l europe-west1 gs://csc1142-projet/

# Create structure
gsutil mkdir gs://csc1142-projet/raw/
gsutil mkdir gs://csc1142-projet/processed/
gsutil mkdir gs://csc1142-projet/scripts/
```

### 5. Create Dataproc cluster

```bash
gcloud dataproc clusters create csc1142-spark-cluster \
  --region=europe-west1 \
  --zone=europe-west1-b \
  --master-machine-type=n1-standard-2 \
  --master-boot-disk-size=50GB \
  --num-workers=2 \
  --worker-machine-type=n1-standard-2 \
  --worker-boot-disk-size=50GB \
  --image-version=2.2-debian12 \
  --project=regal-sun-478114-q5 \
  --max-idle=1800s
```

## Usage

### Complete pipeline (recommended)

```bash
# 1. Automated data ingestion (Cloud Functions + Scheduler)
./cloud_functions/deploy_all.sh  # Deploy once
gcloud scheduler jobs run ingest-carburants-scheduler --location=europe-west1

# OR Manual ingestion
python data_ingestion/fetch_carburants.py

# 2. Upload Spark scripts to GCS and run processing
gsutil cp spark_jobs/*.py gs://csc1142-projet/scripts/

gcloud dataproc jobs submit pyspark \
  gs://csc1142-projet/scripts/transform_carburants.py \
  --cluster=csc1142-spark-cluster \
  --region=europe-west1 \
  --py-files=gs://csc1142-projet/scripts/utils_spark.py

# 3. Launch visualization dashboard
cd visualization
streamlit run dashboard.py
```

### Step-by-step execution

#### 1. Data ingestion

```bash
cd data_ingestion
python fetch_carburants.py
```

**Output**:
```
Successful ingestion: 50,913 records
File: gs://csc1142-projet/raw/carburants/carburants_20251127_181500.json
```

#### 2. Spark processing

**Option A: Local (development)**
```bash
cd spark_jobs
python transform_carburants.py
```

**Option B: Dataproc (production)**
```bash
gcloud dataproc jobs submit pyspark \
  gs://csc1142-projet/scripts/transform_carburants.py \
  --cluster=csc1142-spark-cluster \
  --region=europe-west1
```

**Output**:
```
Total stations processed: 48,793
Regions: 13
Departments: 97
Period covered: 2007 - 2025 (11 years)
Stations with anomalies: 123
```

#### 3. Launch Visualization Dashboard

```bash
cd visualization
streamlit run dashboard.py
```

The dashboard will open at `http://localhost:8501` with interactive visualizations.

**Direct Data Access** (optional):
```python
import pandas as pd

df = pd.read_parquet("gs://csc1142-projet/processed/carburants/aggregations/by_region/")
print(df.head())
print(f"National average price: {df['avg_gazole'].mean():.3f} €/L")
```

## Data Schemas

### Detailed data (detailed/)

50+ columns including:
- Identifiers: `id`, `ville`, `adresse`, `cp`, `departement`, `region_code`
- Coordinates: `latitude`, `longitude`
- Prices: `gazole`, `sp95`, `sp98`, `e10`, `e85`, `gplc` (€/L)
- Features: `prix_moyen_station`, `{fuel}_moy_region`, `{fuel}_diff_region`, `{fuel}_pct_vs_region`
- Indicators: `competitive_{fuel}`, `competitivity_score`
- Evolution: `{fuel}_evolution` (vs last update)
- Anomalies: `is_outlier_{fuel}`, `has_any_anomaly`
- Metadata: `prix_maj_date`, `processing_timestamp`, `processing_date`

### Aggregations

Each aggregation contains:
- Prices: `{fuel}_moyen`, `{fuel}_min`, `{fuel}_max`, `{fuel}_stddev`
- Counters: `nombre_stations`


## Technologies Used

| Technology | Version | Usage |
|------------|---------|-------|
| Apache Spark | 3.5.0 | Distributed processing |
| PySpark | 3.5.0 | Python Spark API |
| Google Cloud Storage | - | Cloud storage |
| Google Cloud Dataproc | 2.2 | Managed Spark clusters |
| Google Cloud Functions | Gen2 | Serverless automation |
| Cloud Scheduler | - | Scheduled job execution |
| Streamlit | 1.29.0 | Interactive dashboards |
| Plotly | 5.18.0 | Data visualization |
| Folium | 0.15.0+ | Interactive maps |
| Pandas | 2.1.4 | Data manipulation |
| PyArrow | 14.0.1 | Parquet format |

## Data Source

**Government Public API**:
- Source: Ministry of Economy (data.gouv.fr)
- Dataset: Fuel prices in France (instant feed)
- URL: https://data.economie.gouv.fr/api/explore/v2.1/catalog/datasets/prix-des-carburants-en-france-flux-instantane-v2
- Update: Daily
- License: Open License
- Coverage: ~48,000 gas stations in France

## Documentation

- [Cloud Functions Guide](cloud_functions/README.md) - Serverless automation and deployment
- [Spark Pipeline Guide](spark_jobs/README.md) - Complete technical documentation
- [Visualization Dashboard](visualization/) - Interactive Streamlit application

## Maintenance

### Restart the pipeline

```bash
# Delete processed data
gsutil -m rm -r gs://csc1142-projet/processed/carburants/*

# Restart processing
python spark_jobs/transform_carburants.py
```

### Delete Dataproc cluster

```bash
gcloud dataproc clusters delete csc1142-spark-cluster \
  --region=europe-west1 \
  --quiet
```

## Troubleshooting

### Error: "Permission denied"
```bash
gcloud auth application-default login
gcloud config set project regal-sun-478114-q5
```

### Error: "File not found"
```bash
# Check data
gsutil ls gs://csc1142-projet/raw/carburants/
gsutil ls gs://csc1142-projet/processed/carburants/
```

### Slow Spark job
```bash
# Increase cluster resources
gcloud dataproc clusters update csc1142-spark-cluster \
  --region=europe-west1 \
  --num-workers=4
```

## Authors

Yanis Boumedad && Mattéo Duprat

## License

Educational purposes only - Public data under Open License
