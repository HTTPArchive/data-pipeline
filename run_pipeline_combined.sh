#!/bin/bash
# shellcheck disable=SC1143,SC2211,SC2215
python3 run_combined.py \
  --input_file=gs://httparchive/crawls_manifest/chrome-Feb_1_2024.txt \
  --runner=DataflowRunner \
  --project=httparchive \
  --temp_location=gs://httparchive-staging/experimental/temp \
  --staging_location=gs://httparchive-staging/experimental/staging \
  --region=us-west1 \
  --setup_file=./setup.py \
  --machine_type=n1-standard-32 \
  --worker_disk_type=compute.googleapis.com/projects//zones//diskTypes/pd-ssd \
  --max_cache_memory_usage_mb=0
