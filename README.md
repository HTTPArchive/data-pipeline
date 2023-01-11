# data-pipeline
The new HTTP Archive data pipeline built entirely on GCP

![GitHub branch checks state](https://github.com/HTTPArchive/data-pipeline/actions/workflows/code-static-analysis.yml/badge.svg?branch=main)
![GitHub branch checks state](https://github.com/HTTPArchive/data-pipeline/actions/workflows/linter.yml/badge.svg?branch=main)
![GitHub branch checks state](https://github.com/HTTPArchive/data-pipeline/actions/workflows/unittest.yml/badge.svg?branch=main)
![Coverage badge](https://img.shields.io/endpoint?url=https://raw.githubusercontent.com/wiki/HTTPArchive/data-pipeline/python-coverage-comment-action-badge.json)

- [Run the pipeline](#run-the-pipeline)
  * [Locally using the `run_pipeline*.sh` scripts](#locally-using-the--run-pipeline-sh--scripts)
  * [Running a flex template from the Cloud Console](#running-a-flex-template-from-the-cloud-console)
  * [Publishing a Pub/Sub message](#publishing-a-pub-sub-message)
  * [Pipeline types](#pipeline-types)
- [Inputs](#inputs)
  * [Generating HAR manifest files](#generating-har-manifest-files)
- [Outputs](#outputs)
- [Temp table cleanup](#temp-table-cleanup)
- [Known issues](#known-issues)
  * [Dataflow](#dataflow)
    + [Logging](#logging)
  * [Response cache-control max-age](#response-cache-control-max-age)
  * [New file formats](#new-file-formats)
  * [mimetypes and file extensions](#mimetypes-and-file-extensions)

<small><i><a href='http://ecotrust-canada.github.io/markdown-toc/'>Table of contents generated with markdown-toc</a></i></small>



## Run the pipeline
Dataflow jobs can be triggered several ways:
- Locally using bash scripts
- From the Google Cloud Console
- By publishing a Pub/Sub message

### Locally using the `run_pipeline*.sh` scripts

This method is best used when developing locally, as a convenience for running the pipeline's python scripts and GCP CLI commands.

```shell
# run the pipeline locally
./run_pipeline_combined.sh
./run_pipeline_all.sh

# run the pipeline using a flex template
./run_flex_template all [...]
./run_flex_template combined [...]
```

### Running a flex template from the Cloud Console

**TODO: ADD DETAILS**

This method is useful for running individual dataflow jobs from the web console since it does not require a development environment.

Flex templates accept additional parameters as mentioned in the GCP documentation below, while custom parameters are defined in `flex_template_metadata_*.json`

https://cloud.google.com/dataflow/docs/guides/templates/configuring-flex-templates#specify-options

https://cloud.google.com/dataflow/docs/guides/templates/using-flex-templates#run-a-flex-template-pipeline


### Publishing a Pub/Sub message

This method is best used for serverlessly running the entire workflow, including logic to
- block execution when the crawl is still running, by waiting for the crawl's Pub/Sub queue to drain
- skip jobs where BigQuery tables have already been populated
- automaticly retry failed jobs

Publishing a message containing the crawl's GCS path will trigger a GCP workflow.

``` shell
gcloud pubsub topics publish projects/httparchive/topics/crawl-complete --message "gs://httparchive/crawls/android-Nov_1_2022"

```


### Pipeline types

Running the `combined` pipeline will produce summary and non-summary tables by default.
Summary and non-summary outputs can be controlled using the `--pipeline_type` argument.

```shell
# example
./run_pipeline_combined.sh --pipeline_type=summary

./run_flex_template.sh combined --parameters pipeline_type=summary
```

## Inputs

This pipeline can read individual HAR files, or a single file containing a list of HAR file paths.

```shell
# Run the `all` pipeline on both desktop and mobile using their pre-generated manifests.
./run_flex_template.sh all --parameters input_file=gs://httparchive/crawls_manifest/*-Nov_1_2022.txt

# Run the `combined` pipeline on mobile using its manifest.
./run_flex_template.sh combined --parameters input_file=gs://httparchive/crawls_manifest/android-Nov_1_2022.txt

# Run the `combined` pipeline on desktop using its individual HAR files (much slower, not encouraged).
./run_flex_template.sh combined --parameters input=gs://httparchive/crawls/chrome-Nov_1_2022
```

### Generating HAR manifest files

The pipeline can read a manifest file (text file containing GCS file paths separated by new lines for each HAR file). Follow the example to generate a manifest file:

```shell
# generate manifest files
nohup gsutil ls gs://httparchive/crawls/chrome-Nov_1_2022 > chrome-Nov_1_2022.txt 2> chrome-Nov_1_2022.err &
nohup gsutil ls gs://httparchive/crawls/android-Nov_1_2022 > android-Nov_1_2022.txt 2> android-Nov_1_2022.err &

# watch for completion (i.e. file sizes will stop changing)
#   if the err file increases in size, open and check for issues
watch ls -l ./*Nov*

# upload to GCS
gsutil -m cp ./*Nov*.txt gs://httparchive/crawls_manifest/
```

## Outputs

- GCP DataFlow & Monitoring metrics - TODO: runtime metrics and dashboards
- Dataflow temporary and staging artifacts in GCS
- BigQuery (final landing zone)

## Temp table cleanup

Since this pipeline uses the `FILE_LOADS` BigQuery insert method, failures will leave behind temporary tables.
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

### Response cache-control max-age

Various parsing issues due to unhandled cases

### New file formats

New file formats from responses will be noted in WARNING logs

### mimetypes and file extensions

Using ported custom logic from legacy PHP rather than standard libraries produces missing values and inconsistencies
