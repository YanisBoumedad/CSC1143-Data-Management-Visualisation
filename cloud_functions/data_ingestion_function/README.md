# Cloud Function - Automatic Fuel Price Ingestion

This Cloud Function enables automatic daily ingestion of fuel price data from the government API to Google Cloud Storage.

## Architecture

This Cloud Function **reuses existing project code**:
- `data_ingestion/fetch_carburants.py`: Ingestion logic
- `data_ingestion/utils.py`: GCS upload and metadata
- `config/settings.py`: Project configuration

The `main.py` file is a simple wrapper that calls these existing functions.

## File Structure

```
cloud_functions/
├── prepare_deployment.sh          # Deployment preparation script
└── data_ingestion_function/
    ├── main.py                    # Entry point (minimal wrapper)
    ├── requirements.txt           # Dependencies
    ├── .gcloudignore              # Files to exclude
    └── README.md                  # This file
```

## Deployment Preparation

Before deploying, you need to copy necessary modules (`data_ingestion/` and `config/`) into the Cloud Function folder:

```bash
# From project root
./cloud_functions/prepare_deployment.sh
```

This script automatically copies:
- `data_ingestion/` → `cloud_functions/data_ingestion_function/data_ingestion/`
- `config/` → `cloud_functions/data_ingestion_function/config/`

## Deployment

### 1. Prepare files

```bash
./cloud_functions/prepare_deployment.sh
```

### 2. Navigate to folder

```bash
cd cloud_functions/data_ingestion_function
```

### 3. Deploy function

```bash
gcloud functions deploy ingest-carburants-daily \
  --gen2 \
  --runtime=python311 \
  --region=europe-west1 \
  --source=. \
  --entry-point=ingest_carburants \
  --trigger-http \
  --allow-unauthenticated \
  --service-account=data-ingestion-sa@regal-sun-478114-q5.iam.gserviceaccount.com \
  --timeout=540s \
  --memory=512MB
```

## How It Works

### Entry Point: `ingest_carburants(request)`

This function is automatically triggered by Cloud Scheduler (HTTP trigger).

### Ingestion Pipeline

The `main.py` calls existing functions:

1. `fetch_carburants_data()`: Retrieval from API
2. `process_carburants_data()`: Enrichment + normalization
3. `upload_to_gcs()`: Upload to GCS with metadata

### Output

```
gs://csc1142-projet/raw/carburants/
├── carburants_YYYY-MM-DD.json
└── carburants_YYYY-MM-DD_metadata.json
```

## Manual Testing

### Test function after deployment

```bash
gcloud functions call ingest-carburants-daily \
  --region=europe-west1 \
  --gen2
```

### Check logs

```bash
gcloud functions logs read ingest-carburants-daily \
  --region=europe-west1 \
  --gen2 \
  --limit=50
```

### Check GCS files

```bash
gsutil ls -lh gs://csc1142-projet/raw/carburants/ | tail -5
```

## Function Response

### Success (HTTP 200)

```json
{
  "success": true,
  "message": "Successful ingestion",
  "gcs_uri": "gs://csc1142-projet/raw/carburants/carburants_2025-11-28.json",
  "record_count": 50913,
  "timestamp": "2025-11-28T02:00:00.123456"
}
```

### Failure (HTTP 500)

```json
{
  "success": false,
  "message": "Ingestion failed: [error message]",
  "timestamp": "2025-11-28T02:00:00.123456"
}
```

## Advantages of this Approach

- **DRY (Don't Repeat Yourself)**: No code duplication
- **Easy maintenance**: Single source of truth
- **Consistency**: Same logic locally and in cloud
- **Testability**: Code can be tested locally before deployment

## Monitoring

### View executions

```bash
gcloud functions describe ingest-carburants-daily \
  --region=europe-west1 \
  --gen2
```

### Alerts

Errors are automatically logged in Cloud Logging and can trigger Cloud Monitoring alerts.

## Resources

- **Runtime**: Python 3.11
- **Memory**: 512 MB
- **Timeout**: 9 minutes (540s)
- **Region**: europe-west1

## Estimated Cost

- Approximately 0.40€/month for 31 daily executions of ~30 seconds

## Cleanup

To delete the function:

```bash
gcloud functions delete ingest-carburants-daily \
  --region=europe-west1 \
  --gen2 \
  --quiet
```
