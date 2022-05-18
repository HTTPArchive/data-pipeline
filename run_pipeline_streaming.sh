#!/bin/bash
# shellcheck disable=SC1143,SC2211,SC2215

REGION=us-west1

if [[ -n "$1" ]]; then
  check=$(gcloud dataflow jobs list --region $REGION --status=active --filter="name=$1 AND type=Streaming" --format="value(name)")
  if [[ -z "$check" ]]; then
    echo Running streaming job not found!
    exit 1
  fi
  update_args="--update --job_name=$1"
fi


python3 run_pipeline.py \
  "${update_args}" \
  --subscription=projects/httparchive/subscriptions/har-gcs-pipeline \
  --streaming \
  --save_main_session \
  --temp_location=gs://httparchive-staging/experimental/temp \
  --staging_location=gs://httparchive-staging/experimental/staging \
  --enable_streaming_engine \
  --setup_file=./setup.py \
  --runner=DataflowRunner \
  --project=httparchive \
  --region=$REGION \
  --machine_type=n1-standard-32 \
  --worker_disk_type=compute.googleapis.com/projects//zones//diskTypes/pd-ssd
