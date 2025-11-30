#!/bin/bash

# Script de déploiement complet de la Cloud Function
# 1. Copie les fichiers nécessaires
# 2. Déploie la fonction

set -e  # Arrêter en cas d'erreur

SCRIPT_DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )"
PROJECT_ROOT="$SCRIPT_DIR/.."
FUNCTION_DIR="$SCRIPT_DIR/data_ingestion_function"

echo "=============================================================="
echo "DÉPLOIEMENT CLOUD FUNCTION - INGESTION CARBURANTS"
echo "=============================================================="
echo ""

# Étape 1 : Préparation des fichiers
echo "[1/2] Préparation des fichiers..."
echo "--------------------------------------------------------------"

# Copier data_ingestion
echo "  Copie de data_ingestion/..."
cp -r "$PROJECT_ROOT/data_ingestion" "$FUNCTION_DIR/"

# Copier config
echo "  Copie de config/..."
cp -r "$PROJECT_ROOT/config" "$FUNCTION_DIR/"

echo "  ✓ Fichiers copiés"
echo ""

# Étape 2 : Déploiement
echo "[2/2] Déploiement sur Google Cloud..."
echo "--------------------------------------------------------------"

cd "$FUNCTION_DIR"

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

echo ""
echo "=============================================================="
echo "✓ DÉPLOIEMENT TERMINÉ !"
echo "=============================================================="
echo ""
echo "Pour tester la fonction :"
echo "  gcloud functions call ingest-carburants-daily --region=europe-west1 --gen2"
echo ""
echo "Pour voir les logs :"
echo "  gcloud functions logs read ingest-carburants-daily --region=europe-west1 --gen2 --limit=50"
