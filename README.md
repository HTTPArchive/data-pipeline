# data-pipeline
The new HTTP Archive data pipeline built entirely on GCP

![GitHub branch checks state](https://github.com/HTTPArchive/data-pipeline/actions/workflows/code-static-analysis.yml/badge.svg?branch=main)
![GitHub branch checks state](https://github.com/HTTPArchive/data-pipeline/actions/workflows/linter.yml/badge.svg?branch=main)
![GitHub branch checks state](https://github.com/HTTPArchive/data-pipeline/actions/workflows/unittest.yml/badge.svg?branch=main)
![Coverage badge](https://img.shields.io/endpoint?url=https://raw.githubusercontent.com/wiki/HTTPArchive/data-pipeline/python-coverage-comment-action-badge.json)

- [Run the pipeline](#run-the-pipeline)
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

```shell
./run_pipeline_batch.sh
```

### Pipeline types

By default, running the pipeline will run in "combined" mode to produce summary and non-summary tables.
This can be controlled using the `--pipeline_type` argument.

```shell
# example
./run_pipeline_batch.sh --pipeline_type=summary
```

## Inputs

This pipeline can read individual HAR files, or a single file containing a list of HAR file paths.

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
