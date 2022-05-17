#!/bin/bash
# shellcheck disable=SC1143,SC2211,SC2215
python3 run_pipeline.py \
  --subscription=projects/httparchive/subscriptions/har-gcs-pipeline \
  --streaming \
  --save_main_session \
  --temp_location=gs://httparchive-staging/experimental/temp \
  --staging_location=gs://httparchive-staging/experimental/staging \
  --enable_streaming_engine \
  --setup_file=./setup.py \
  --runner=DataflowRunner \
  --project=httparchive \
  --region=us-west1 \
  --machine_type=n1-standard-32 \
  --worker_disk_type=compute.googleapis.com/projects//zones//diskTypes/pd-ssd \
