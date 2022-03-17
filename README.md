# data-pipeline
The new HTTP Archive data pipeline built entirely on GCP

# Initial setup
TODO: follow known instructions

```shell
# Install GCP SDK and authenticate
gcloud init
gcloud auth login
gcloud config set project httparchive

PROJECT=gcloud config get-value project
TOPIC=har-gcs
SUBSCRIPTION=har-gcs-pipeline
INPUT_PATH=experimental/input/
BUCKET=gs://httparchive


# Create a Pub/Sub topic, subscription, and GCS notifications
# topic is used to monitor file creation events in Google Cloud Storge

gcloud pubsub topics create $TOPIC
gcloud pubsub subscriptions create $SUBSCRIPTION --topic=$TOPIC
gsutil notification create \
    -f json \
    -e OBJECT_FINALIZE \
    -t projects/$PROJECT/topics/$TOPIC \
    -p $INPUT_PATH \
    $BUCKET
```

# Checking Pub/Sub notifications
```shell
gcloud pubsub subscriptions pull projects/$PROJECT/subscriptions/$SUBSCRIPTION
```

# Run the pipeline
## batch
```commandline
python import_har.py \
--input=gs://httparchive/experimental/input/* \
--temp_location=gs://httparchive/experimental/temp \
--staging_location=gs://httparchive/experimental/staging \
--setup_file="D:\development\projects\HTTPArchive\data-pipeline\setup.py" \
--runner=DataflowRunner \
--project=httparchive \
--region=us-west1 \
--machine_type=n1-standard-32 \
--worker_disk_type=compute.googleapis.com/projects//zones//diskTypes/pd-ssd
```

## streaming
```commandline
python import_har.py \
--streaming \
--enable_streaming_engine \
--experiments=use_runner_v2 \
--temp_location=gs://httparchive/experimental/temp \
--staging_location=gs://httparchive/experimental/staging \
--setup_file="D:\development\projects\HTTPArchive\data-pipeline\setup.py" \
--runner=DataflowRunner \
--project=httparchive \
--region=us-west1 \
--machine_type=n1-standard-32 \
--worker_disk_type=compute.googleapis.com/projects//zones//diskTypes/pd-ssd
```

# Inputs

- GCS -> Pub/Sub

# Outputs

- Pub/Sub - TODO: update topics when files have been processed
- GCP DataFlow & Monitoring metrics - TODO: runtime metrics and dashboards
- GCS - store ELT results (e.g. summary pages JSONL) temporarily with TTL
- BigQuery - final landing zone