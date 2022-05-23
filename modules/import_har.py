import argparse
import logging

import apache_beam as beam
from apache_beam.io.gcp.bigquery import BigQueryWriteFn
from apache_beam.options.pipeline_options import PipelineOptions, StandardOptions
from apache_beam.runners import DataflowRunner

from modules import constants, utils
from modules.transformation import ImportHarJson, ReadHarFiles, WriteBigQuery


def parse_args(argv):
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--input",
        dest="input",
        help="Input file to process. Example: gs://httparchive/crawls/*Jan_1_2022"
    )
    parser.add_argument(
        "--subscription",
        dest="subscription",
        help="Pub/Sub subscription. Example: `projects/httparchive/subscriptions/har-gcs-pipeline`",
    )
    return parser.parse_known_args(argv)


def run(argv=None):
    known_args, pipeline_args = parse_args(argv)
    pipeline_options = PipelineOptions(pipeline_args, save_main_session=True)
    standard_options = pipeline_options.view_as(StandardOptions)
    if not (known_args.subscription or known_args.input):
        raise RuntimeError(
            "Either one of --input or --subscription options must be provided"
        )

    # TODO log and persist execution arguments to storage for tracking
    # https://beam.apache.org/documentation/patterns/pipeline-options/

    # TODO add metric counters for files in, processed, written to GCP & BQ

    p = beam.Pipeline(options=pipeline_options)

    parsed = (
        p
        | ReadHarFiles(known_args.subscription, known_args.input)
        | "ParseHar" >> beam.ParDo(ImportHarJson()).with_outputs("page", "requests")
    )
    pages, requests = parsed
    requests = requests | "FlattenRequests" >> beam.FlatMap(
        lambda elements: elements
    )

    pages_errors = pages | "WritePagesToBigQuery" >> WriteBigQuery(
        table=lambda row: utils.format_table_name(row, "pages"),
        schema=constants.bigquery["schemas"]["pages"],
        streaming=standard_options.streaming,
    )

    requests_errors = requests | "WriteRequestsToBigQuery" >> WriteBigQuery(
        table=lambda row: utils.format_table_name(row, "requests"),
        schema=constants.bigquery["schemas"]["requests"],
        streaming=standard_options.streaming,
    )

    # deadletter logging
    if standard_options.streaming:
        (
            pages_errors[BigQueryWriteFn.FAILED_ROWS]
            | "PrintPagesErrors"
            >> beam.FlatMap(
                lambda e: logging.error(f"Could not load page to BigQuery: {e}")
            )
        )
        (
            requests_errors[BigQueryWriteFn.FAILED_ROWS]
            | "PrintRequestsErrors"
            >> beam.FlatMap(
                lambda e: logging.error(f"Could not load request to BigQuery: {e}")
            )
        )

    # TODO implement deadletter for FILE_LOADS?
    #  FAILED_ROWS not implemented for BigQueryBatchFileLoads in this version of beam (only _StreamToBigQuery)
    # pages_result[BigQueryBatchFileLoads.DESTINATION_JOBID_PAIRS] | beam.Map(lambda e: print(f"jobidpair: {e}"))
    # pages_result[BigQueryBatchFileLoads.DESTINATION_FILE_PAIRS] | beam.Map(lambda e: print(f"files: {e}"))
    # pages_result[BigQueryBatchFileLoads.DESTINATION_COPY_JOBID_PAIRS] | beam.Map(lambda e: print(f"copies: {e}"))

    # TODO detect DONE file, move temp table to final destination, shutdown pipeline (if streaming)

    pipeline_result = p.run()
    if not isinstance(p.runner, DataflowRunner):
        pipeline_result.wait_until_finish()
