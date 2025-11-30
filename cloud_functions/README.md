# Automatic Ingestion Pipeline - Cloud Functions + Cloud Scheduler

Serverless pipeline for daily automatic ingestion of fuel prices in France to Google Cloud Storage.

## Overview

This solution uses **Cloud Functions** (Gen2) and **Cloud Scheduler** to automate daily data retrieval from the government API.

### Architecture

```
Cloud Scheduler (daily cron at 2am)
         ↓ HTTP POST
Cloud Function (ingest-carburants-daily)
         ↓ fetch
Government API (data.gouv.fr)
         ↓ process + enrich
Enriched data (timestamps, normalization)
         ↓ upload
Google Cloud Storage (gs://csc1142-projet/raw/carburants/)
```

### Advantages of this approach

- **Serverless**: No server to manage
- **Automatic**: Daily execution without intervention
- **Economical**: Pay-per-use (~1€/month)
- **Scalable**: Automatic auto-scaling
- **Resilient**: Automatic retry on failure

## Prerequisites

### GCP Account

- GCP Project: `regal-sun-478114-q5`
- GCS Bucket: `csc1142-projet`
- Enabled APIs:
  - Cloud Functions API
  - Cloud Scheduler API
  - Cloud Build API
  - Cloud Run API
  - Cloud Storage API

### Local Tools

```bash
# Google Cloud SDK
gcloud version

# Python 3.11+
python3 --version
```

### Permissions

Service Account: `data-ingestion-sa@regal-sun-478114-q5.iam.gserviceaccount.com`

Required roles:
- `roles/storage.objectAdmin` (GCS write)
- `roles/cloudfunctions.invoker` (Cloud Function execution)

## Project Structure

```
cloud_functions/
├── README.md                           # This file
├── deploy_all.sh                       # Complete deployment script
├── prepare_deployment.sh               # Preparation script (optional)
└── data_ingestion_function/
    ├── main.py                         # Cloud Function entry point
    ├── requirements.txt                # Python dependencies
    ├── .gcloudignore                   # Files excluded from deployment
    ├── README.md                       # Function documentation
    ├── data_ingestion/                 # ← Copied from ../../data_ingestion/
    │   ├── __init__.py
    │   ├── fetch_carburants.py         # Ingestion logic
    │   └── utils.py                    # GCS upload and metadata
    └── config/                         # ← Copied from ../../config/
        ├── __init__.py
        └── settings.py                 # Project configuration
```

## Installation and Configuration

### 1. GCP Authentication

```bash
gcloud auth login
gcloud auth application-default login
gcloud config set project regal-sun-478114-q5
```

### 2. Enable Required APIs

```bash
gcloud services enable cloudfunctions.googleapis.com
gcloud services enable cloudscheduler.googleapis.com
gcloud services enable cloudbuild.googleapis.com
gcloud services enable run.googleapis.com
gcloud services enable storage.googleapis.com
```

### 3. Create Service Account (if not already done)

```bash
# Create the service account
gcloud iam service-accounts create data-ingestion-sa \
  --description="Service account for automated data ingestion" \
  --display-name="Data Ingestion Service Account"

# Grant Storage permissions
gcloud projects add-iam-policy-binding regal-sun-478114-q5 \
  --member="serviceAccount:data-ingestion-sa@regal-sun-478114-q5.iam.gserviceaccount.com" \
  --role="roles/storage.objectAdmin"

# Grant Cloud Functions permissions
gcloud projects add-iam-policy-binding regal-sun-478114-q5 \
  --member="serviceAccount:data-ingestion-sa@regal-sun-478114-q5.iam.gserviceaccount.com" \
  --role="roles/cloudfunctions.invoker"
```

### 4. Verify GCS Bucket

```bash
gsutil ls gs://csc1142-projet/raw/carburants/
```

If folder doesn't exist:
```bash
gsutil mkdir gs://csc1142-projet/raw/carburants/
```

## Deployment

### Option 1: Automatic Deployment (recommended)

The `deploy_all.sh` script does everything automatically:
1. Copies necessary modules (`data_ingestion/` and `config/`)
2. Deploys the Cloud Function with correct configurations

```bash
# From project root
./cloud_functions/deploy_all.sh
```

### Option 2: Manual Deployment

```bash

# 1. Navigate to folder
cd cloud_functions/data_ingestion_function

# 2. Deploy
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
  --memory=2048MB
```

Deployment takes **5-10 minutes**.

### Deployment Verification

```bash
# Check function status
gcloud functions describe ingest-carburants-daily \
  --region=europe-west1 \
  --gen2

# Test manually
gcloud functions call ingest-carburants-daily \
  --region=europe-west1 \
  --gen2
```

## Cloud Scheduler Configuration

### Create scheduler job

```bash
gcloud scheduler jobs create http ingest-carburants-scheduler \
  --location=europe-west1 \
  --schedule="0 2 * * *" \
  --uri="https://europe-west1-regal-sun-478114-q5.cloudfunctions.net/ingest-carburants-daily" \
  --http-method=POST \
  --time-zone="Europe/Paris" \
  --description="Daily fuel price ingestion"
```

### Cron format explanation

```
0 2 * * *
│ │ │ │ │
│ │ │ │ └─── Day of week (0-6, Sunday=0)
│ │ │ └───── Month (1-12)
│ │ └─────── Day of month (1-31)
│ └───────── Hour (0-23)
└─────────── Minute (0-59)

= Every day at 2:00 AM (Paris time)
```

### Modify schedule (optional)

```bash
# Change to 3am
gcloud scheduler jobs update http ingest-carburants-scheduler \
  --location=europe-west1 \
  --schedule="0 3 * * *"

# Execute only on weekdays (Monday-Friday)
gcloud scheduler jobs update http ingest-carburants-scheduler \
  --location=europe-west1 \
  --schedule="0 2 * * 1-5"
```

## Usage

### Test scheduler manually

Trigger immediate execution (without waiting for 2am):

```bash
gcloud scheduler jobs run ingest-carburants-scheduler \
  --location=europe-west1
```

### Check logs

#### Cloud Function logs

```bash
gcloud functions logs read ingest-carburants-daily \
  --region=europe-west1 \
  --gen2 \
  --limit=50
```

#### Real-time logs (streaming)

```bash
gcloud functions logs tail ingest-carburants-daily \
  --region=europe-west1 \
  --gen2
```

### Check created files

```bash
# List most recent files
gsutil ls -lh gs://csc1142-projet/raw/carburants/ | tail -10

# Display file content
gsutil cat gs://csc1142-projet/raw/carburants/carburants_2025-11-28.json | head -50
```

## Monitoring

### Cloud Scheduler Status

```bash
# View all jobs
gcloud scheduler jobs list --location=europe-west1

# Specific job details
gcloud scheduler jobs describe ingest-carburants-scheduler \
  --location=europe-west1
```

### Cloud Functions Metrics

```bash
# Number of executions
gcloud logging read "resource.type=cloud_function \
  AND resource.labels.function_name=ingest-carburants-daily" \
  --limit=10 \
  --format=json
```

### GCP Console

- [Cloud Functions Dashboard](https://console.cloud.google.com/functions/details/europe-west1/ingest-carburants-daily?project=regal-sun-478114-q5)
- [Cloud Scheduler Jobs](https://console.cloud.google.com/cloudscheduler?project=regal-sun-478114-q5)
- [Cloud Storage Browser](https://console.cloud.google.com/storage/browser/csc1142-projet/raw/carburants?project=regal-sun-478114-q5)

## Maintenance

### Update Cloud Function

If you modify source code in `data_ingestion/`, redeploy:

```bash
./cloud_functions/deploy_all.sh
```

### Suspend automatic ingestion

```bash
# Disable scheduler
gcloud scheduler jobs pause ingest-carburants-scheduler \
  --location=europe-west1

# Re-enable
gcloud scheduler jobs resume ingest-carburants-scheduler \
  --location=europe-west1
```

### Delete infrastructure

```bash
# Delete scheduler
gcloud scheduler jobs delete ingest-carburants-scheduler \
  --location=europe-west1 \
  --quiet

# Delete Cloud Function
gcloud functions delete ingest-carburants-daily \
  --region=europe-west1 \
  --gen2 \
  --quiet
```

## Troubleshooting

### Error: "Memory limit exceeded"

Increase memory in `deploy_all.sh`:

```bash
# Modify the --memory line
--memory=4096MB  # 4 GB instead of 2 GB
```

Then redeploy.

### Error: "Service Unavailable (503)"

Possible causes:
1. **Cold start**: Function is starting (normal, wait 30s)
2. **Import error**: Check logs
3. **Timeout**: Increase `--timeout`

Check detailed logs:
```bash
gcloud functions logs read ingest-carburants-daily \
  --region=europe-west1 \
  --gen2 \
  --limit=50
```

### Error: "Permission denied"

Verify service account has correct permissions:

```bash
gcloud projects get-iam-policy regal-sun-478114-q5 \
  --flatten="bindings[].members" \
  --filter="bindings.members:data-ingestion-sa@regal-sun-478114-q5.iam.gserviceaccount.com"
```

### Scheduler not executing

```bash
# Check status
gcloud scheduler jobs describe ingest-carburants-scheduler \
  --location=europe-west1

# Check next execution
gcloud scheduler jobs describe ingest-carburants-scheduler \
  --location=europe-west1 \
  --format="value(scheduleTime)"
```

### Empty or missing logs

Increase logging level in `main.py`:
```python
logging.basicConfig(level=logging.DEBUG)
```

## Costs

### Monthly Estimate

| Service | Usage | Monthly Cost |
|---------|-------|--------------|
| Cloud Functions | 31 executions × 60s × 2GB | ~0.80€ |
| Cloud Scheduler | 1 job (free under 3 jobs) | 0€ |
| Cloud Storage | ~100 MB × 31 days | ~0.02€ |
| **Total** | | **~0.82€/month** |

### Cost Optimizations

1. **Reduce memory** if possible (test with 1 GB)
2. **Reduce timeout** (test with 300s)
3. **Clean old files** GCS (lifecycle policy)

```bash
# Create deletion policy after 90 days
gsutil lifecycle set lifecycle.json gs://csc1142-projet
```

File `lifecycle.json`:
```json
{
  "lifecycle": {
    "rule": [
      {
        "action": {"type": "Delete"},
        "condition": {
          "age": 90,
          "matchesPrefix": ["raw/carburants/"]
        }
      }
    ]
  }
}
```

## Possible Evolutions

### 1. Automate Spark Processing

Create a second scheduler that launches Dataproc job after ingestion:

```bash
gcloud scheduler jobs create http spark-transform-scheduler \
  --location=europe-west1 \
  --schedule="0 3 * * *" \
  --uri="https://dataproc.googleapis.com/v1/projects/regal-sun-478114-q5/regions/europe-west1/jobs:submit" \
  --http-method=POST
```

### 2. Email Notifications

Configure Cloud Monitoring alerts to be notified on failure.

### 3. Automatic Retry

Cloud Scheduler already has built-in retry system (max 5 attempts).

## Additional Documentation

- [Cloud Function Guide](data_ingestion_function/README.md)
- [Main Project README](../README.md)
- [Spark Pipeline Guide](../spark_jobs/README.md)

## Support

For any question or problem:

1. Check logs: `gcloud functions logs read ...`
2. Check GCP console
3. Test manually: `gcloud functions call ...`

---

**Academic Project** - Dublin City University (DCU) - Cloud Technologies

Last update: November 28, 2025
