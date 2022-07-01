
# data-pipeline
The new HTTP Archive data pipeline built entirely on GCP

## Initial setup
TODO: follow known instructions
https://beam.apache.org/get-started/quickstart-py/

TODO: python3.8

```shell
# Install GCP SDK and authenticate
gcloud init
gcloud auth login
gcloud config set project httparchive

PROJECT=$(gcloud config get-value project)
TOPIC=har-gcs
SUBSCRIPTION=har-gcs-pipeline
INPUT_PATH=crawls
BUCKET=gs://httparchive


# Create a Pub/Sub topic, subscription, and GCS notifications
# topic is used to monitor file creation events in Google Cloud Storge

gcloud pubsub topics create $TOPIC
gcloud pubsub subscriptions create $SUBSCRIPTION --topic=$TOPIC --expiration-period=never
gsutil notification create \
    -f json \
    -e OBJECT_FINALIZE \
    -t projects/$PROJECT/topics/$TOPIC \
    -p $INPUT_PATH \
    $BUCKET
```

## Checking Pub/Sub notifications
```shell
gcloud pubsub subscriptions pull projects/$PROJECT/subscriptions/$SUBSCRIPTION
```

## Manually backfill from GCS to Pub/Sub
TODO: not viable, too slow

```shell
gsutil ls gs://httparchive/crawls/** | \
sed -r 's/gs:\/\/([^\/]*)\/(.*)/bucketId=\1,objectId=\2/g' | \
while IFS= read -r f; do gcloud pubsub topics publish projects/httparchive/topics/har-gcs --attribute=$f; done
```

## Run the pipeline
### Run from Pub/Sub (streaming)
```shell
./run_pipeline_streaming.sh
```

### Read from GCS (batch)

```shell
./run_pipeline_batch.sh
```

### Pipeline types

By default, running the pipeline will run in "combined" mode to produce summary and non-summary tables.
This can be controlled using the `--pipeline_type` argument on either batch or streaming.

> ⚠ Note: streaming to non-summary tables is only supported in the combined pipeline currently (i.e. not supported in non-summary-only)

```shell
# example
./run_pipeline_batch.sh --pipeline_type=summary
```

## Update the pipeline
### Update streaming
Supply the run script with a currently running job name

> ⚠ Read the documentation carefully to understand the potential effects and limitations before updating in-place
> https://cloud.google.com/dataflow/docs/guides/updating-a-pipeline

```shell
./run_pipeline_streaming.sh beam-app-abc-123-456-def
```

## Inputs

This pipeline can read inputs from two sources
- GCS notifications to Pub/Sub
- GCS file path (globbing is accepted)

## Outputs

- GCP DataFlow & Monitoring metrics - TODO: runtime metrics and dashboards
- Dataflow temporary and staging artifacts in GCS
- BigQuery (final landing zone)

## Known issues

### Dataflow

#### Logging

> The work item requesting state read is no longer valid on the backend

This log message is benign and expected when using an auto-scaling pipeline
https://cloud.google.com/dataflow/docs/guides/common-errors#work-item-not-valid

#### Batch loads vs streaming inserts

Various incompatibilities due to missing features
* missing dead-letter collections for batch loads
* fixed vs auto-sharding

#### RuntimeError: VarLong too long

This is a known issue when using the DirectRunner on Windows 10 with the Beam Python SDK

https://issues.apache.org/jira/browse/BEAM-11037

### Response cache-control max-age

Various parsing issues due to unhandled cases

### New file formats

New file formats from responses will be noted in WARNING logs

### mimetypes and file extensions

Using ported custom logic from legacy PHP rather than standard libraries produces missing values and inconsistencies
