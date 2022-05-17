#!/bin/bash
# shellcheck disable=SC1143,SC2211,SC2215
python3 run_pipeline.py \
  --input=gs://httparchive/crawls/*Jan_1_2022 \
  --runner=DataflowRunner \
  --project=httparchive \
#  --temp_location=gs://httparchive-staging/dataflow/temp \
#  --staging_location=gs://httparchive-staging/dataflow/staging \
  --temp_location=gs://httparchive-staging/experimental/temp \
  --staging_location=gs://httparchive-staging/experimental/staging \
  --region=us-west1 \
  --setup_file=./setup.py
  --machine_type=n1-standard-32 \
  --worker_disk_type=compute.googleapis.com/projects//zones//diskTypes/pd-ssd
