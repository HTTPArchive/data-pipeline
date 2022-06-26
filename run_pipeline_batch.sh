#!/bin/bash
# shellcheck disable=SC1143,SC2211,SC2215
python3 run_pipeline.py \
  --input=gs://httparchive/test-May_24_2022 \
  --runner=DataflowRunner \
  --project=httparchive \
  --temp_location=gs://httparchive-staging/experimental/temp \
  --staging_location=gs://httparchive-staging/experimental/staging \
  --region=us-west1 \
  --setup_file=./setup.py \
  --machine_type=n1-standard-4 \
  --worker_disk_type=compute.googleapis.com/projects//zones//diskTypes/pd-ssd \
  --noauth_local_webserver \
  --pipeline_type=non_summary
