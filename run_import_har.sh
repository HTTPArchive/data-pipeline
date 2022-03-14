#!/bin/bash
python3 import_har.py \
  --runner=DataflowRunner \
  --project=httparchive \
#  --temp_location=gs://httparchive/dataflow/temp \
#  --staging_location=gs://httparchive/dataflow/staging \
  --temp_location=gs://httparchive/experimental/temp \
  --staging_location=gs://httparchive/experimental/staging \
  --region=us-west1 \
  --setup_file=./setup.py
  --machine_type=n1-standard-32 \
#  --input=gs://httparchive/chrome-Jan_1_2022 \
  --input=gs://httparchive/experimental/input/* \
  --worker_disk_type=compute.googleapis.com/projects//zones//diskTypes/pd-ssd