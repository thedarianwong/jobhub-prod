#!/bin/bash
set -e

FUNCTION_DIR="$(cd "$(dirname "$0")/../lambdas/functions" && pwd)"
OUTPUT_DIR="$(cd "$(dirname "$0")/../lambdas" && pwd)/dist"
ZIP_NAME="etl-s3-to-rds.zip"

mkdir -p "$OUTPUT_DIR"
STAGING=$(mktemp -d)
trap "rm -rf $STAGING" EXIT

echo "Installing psycopg2-binary for Amazon Linux (Lambda runtime)..."
docker run --rm \
  --platform linux/amd64 \
  --entrypoint pip \
  -v "$STAGING:/staging" \
  public.ecr.aws/lambda/python:3.11 \
  install psycopg2-binary -t /staging --quiet

echo "Copying Lambda handler..."
cp "$FUNCTION_DIR/EtlS3ToRds.py" "$STAGING/"

echo "Creating ZIP..."
cd "$STAGING"
zip -r "$OUTPUT_DIR/$ZIP_NAME" . -x "*.dist-info/*" -x "__pycache__/*" > /dev/null

echo "Done: $OUTPUT_DIR/$ZIP_NAME ($(du -sh "$OUTPUT_DIR/$ZIP_NAME" | cut -f1))"
