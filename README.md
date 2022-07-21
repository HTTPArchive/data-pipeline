- [data-pipeline](#data-pipeline)
  * [Initial setup](#initial-setup)
  * [Checking Pub/Sub notifications](#checking-pubsub-notifications)
  * [Manually backfill from GCS to Pub/Sub using a manifest file](#manually-backfill-from-gcs-to-pubsub-using-a-manifest-file)
    + [From the CLI](#from-the-cli)
    + [From the Cloud Console](#from-the-cloud-console)
  * [Run the pipeline](#run-the-pipeline)
    + [Run from Pub/Sub (streaming)](#run-from-pubsub-streaming)
    + [Read from GCS (batch)](#read-from-gcs-batch)
    + [Pipeline types](#pipeline-types)
  * [Update the pipeline](#update-the-pipeline)
    + [Update streaming](#update-streaming)
  * [Inputs](#inputs)
  * [Outputs](#outputs)
  * [Temp table cleanup](#temp-table-cleanup)
  * [Known issues](#known-issues)
    + [Dataflow](#dataflow)
      - [Logging](#logging)
      - [Batch loads vs streaming inserts](#batch-loads-vs-streaming-inserts)
      - [RuntimeError: VarLong too long](#runtimeerror-varlong-too-long)
      - [Error 413 (Request Entity Too Large)!!1](#error-413-request-entity-too-large1)
    + [Response cache-control max-age](#response-cache-control-max-age)
    + [New file formats](#new-file-formats)
    + [mimetypes and file extensions](#mimetypes-and-file-extensions)

<small><i><a href='http://ecotrust-canada.github.io/markdown-toc/'>Table of contents generated with markdown-toc</a></i></small>


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
BUCKET=gs://httparchive


# Create a Pub/Sub topic, subscription, and GCS notifications
# topic is used to monitor **file creation** events in Google Cloud Storge for HAR files
TOPIC=har-gcs
SUBSCRIPTION=har-gcs-pipeline
INPUT_PATH=crawls

gcloud pubsub topics create $TOPIC
gcloud pubsub subscriptions create $SUBSCRIPTION --topic=$TOPIC --expiration-period=never
gsutil notification create \
    -f json \
    -e OBJECT_FINALIZE \
    -t projects/$PROJECT/topics/$TOPIC \
    -p $INPUT_PATH \
    $BUCKET

# topic is used to monitor **manual** events in Google Cloud Storge for HAR manifests
TOPIC=har-manifest-gcs
SUBSCRIPTION=har-manifest-gcs-pipeline
INPUT_PATH=crawls_manifest

gcloud pubsub topics create $TOPIC
gcloud pubsub subscriptions create $SUBSCRIPTION --topic=$TOPIC --expiration-period=never
```

## Checking Pub/Sub notifications
```shell
gcloud pubsub subscriptions pull projects/$PROJECT/subscriptions/$SUBSCRIPTION
```

## Manually backfill from GCS to Pub/Sub using a manifest file

Publish a message containing the GCS a file bucket and path name to the manifest Pub/Sub topic (i.e. not the main topic).

Example message body
```json
{"bucket":"httparchive","name":"crawls_manifest/android-Jul_1_2022.txt"}
```

### From the CLI

```shell
gcloud pubsub topics publish projects/httparchive/topics/har-manifest-gcs --message='{"bucket":"httparchive","name":"crawls_manifest/android-Jul_1_2022.txt"}'
```

### From the Cloud Console

1. Navigate to the manifest topic (e.g. `projects/httparchive/topics/har-manifest-gcs`)
2. From the **Messages** tab, select **Publish Message**
3. Enter a value into the the **Message body** and select **Submit**

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

This pipeline can read individual HAR files, or a single file containing a list of HAR file paths
from two sources:
- GCS notifications to Pub/Sub
- GCS file path (globbing is accepted)

## Outputs

- GCP DataFlow & Monitoring metrics - TODO: runtime metrics and dashboards
- Dataflow temporary and staging artifacts in GCS
- BigQuery (final landing zone)

## Temp table cleanup

When running a pipeline using the `FILE_LOADS` BigQuery insert method, failures will leave behind temporary tables.
Use the saved query below and replace the dataset name as desired.

https://console.cloud.google.com/bigquery?sq=226352634162:82dad1cd1374428e8d6eaa961d286559

```sql
FOR field IN
    (SELECT table_schema, table_name
    FROM lighthouse.INFORMATION_SCHEMA.TABLES
    WHERE table_name like 'beam_bq_job_LOAD_%')
DO
    EXECUTE IMMEDIATE format("drop table %s.%s;", field.table_schema, field.table_name);
END FOR;
```

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

#### Error 413 (Request Entity Too Large)!!1

This error is returned by the BigQuery API. Initially encountered when running jobs using `STREAMING_INSERTS`.
While the row size can be controlled in HTTPArchive code, the HTTP request size is controlled in the Beam SDK instead.
When this error occurs, a single row may not actually exceed the quota, but the Beam SDK will batch several
(e.g. 500 rows) together which exceeds the BigQuery HTTP API limit.

One solution is to switch from `STREAMING_INSERTS` to `FILE_LOADS` instead.

At the time of writing, the BigQuery quotas for streaming inserts are:

| Limit                    | Default | Notes                                                                                                                                                                                                                                                                                                                                                                                  |
|--------------------------|---------|----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|
| Maximum row size         | 10 MB   | Exceeding this value causes invalid errors.                                                                                                                                                                                                                                                                                                                                            |
| HTTP request size limit  | 10 MB	  | Exceeding this value causes invalid errors.<br/><br/>Internally the request is translated from HTTP JSON into an internal data structure. The translated data structure has its own enforced size limit. It's hard to predict the size of the resulting internal data structure, but if you keep your HTTP requests to 10 MB or less, the chance of hitting the internal limit is low. |


While the BigQuery quotas for load jobs are:

| Limit                     | Default | Notes                                                                                            |
|---------------------------|---------|--------------------------------------------------------------------------------------------------|
| JSON: Maximum row size    | 100 MB  | JSON rows can be up to 100 MB in size.                                                           |
| Maximum size per load job | 15 TB   | The total size for all of your CSV, JSON, Avro, Parquet, and ORC input files can be up to 15 TB. |

### Response cache-control max-age

Various parsing issues due to unhandled cases

### New file formats

New file formats from responses will be noted in WARNING logs

### mimetypes and file extensions

Using ported custom logic from legacy PHP rather than standard libraries produces missing values and inconsistencies
