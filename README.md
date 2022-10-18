# data-pipeline
The new HTTP Archive data pipeline built entirely on GCP

![GitHub branch checks state](https://github.com/HTTPArchive/data-pipeline/actions/workflows/code-static-analysis.yml/badge.svg?branch=main)
![GitHub branch checks state](https://github.com/HTTPArchive/data-pipeline/actions/workflows/linter.yml/badge.svg?branch=main)
![GitHub branch checks state](https://github.com/HTTPArchive/data-pipeline/actions/workflows/unittest.yml/badge.svg?branch=main)
![Coverage badge](https://img.shields.io/endpoint?url=https://raw.githubusercontent.com/wiki/HTTPArchive/data-pipeline/python-coverage-comment-action-badge.json)

- [Run the pipeline](#run-the-pipeline)
  * [Pipeline types](#pipeline-types)
- [Inputs](#inputs)
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
Dataflow jobs can be triggered locally using the `run_pipeline*.sh` scripts or using glex templates from the Cloud Console.

```shell
# run the pipeline locally
./run_pipeline_combined.sh
./run_pipeline_all.sh

# run the pipeline using a flex template
./run_flex_template all [...]
./run_flex_template combined [...]
```

Flex templates accept additional parameters as mentioned in the GCP documentation below, while custom parameters are defined in `flex_template_metadata_*.json`

https://cloud.google.com/dataflow/docs/guides/templates/configuring-flex-templates#specify-options

https://cloud.google.com/dataflow/docs/guides/templates/using-flex-templates#run-a-flex-template-pipeline


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
