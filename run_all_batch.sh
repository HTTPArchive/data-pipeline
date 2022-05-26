#!/bin/bash
python bigquery_import.py \
  --input=crawls/test-May_25_2022 \
  --runner=DataflowRunner \
  --project=httparchive \
  --temp_location=gs://httparchive-staging/experimental/temp \
  --staging_location=gs://httparchive-staging/experimental/staging \
  --region=us-west1 \
  --setup_file=./setup.py \
  --machine_type=n1-standard-32 \
  --worker_disk_type=compute.googleapis.com/projects//zones//diskTypes/pd-ssd \
  --noauth_local_webserver
