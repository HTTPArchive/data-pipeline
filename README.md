# data-pipeline
The new HTTP Archive data pipeline built entirely on GCP

# Initial setup

## Install GCP SDK and authenticate
TODO: follow known instructions

## Create a Pub/Sub topic and subscription
This Pub/Sub topic is used to monitor file creation events in Google Cloud Storge

```commandline
gcloud pubsub topics create har-gcs
gcloud pubsub subscriptions create har-gcs-pipeline --topic=har-gcs
```

# Run the pipeline
```commandline
python import_har.py
```

# Inputs

- GCS -> Pub/Sub

# Outputs

- Pub/Sub - TODO: update topics when files have been processed
- GCP DataFlow & Monitoring metrics - TODO: runtime metrics and dashboards
- GCS - store ELT results (e.g. summary pages JSONL) temporarily with TTL
- BigQuery - final landing zone