import argparse
import logging

import apache_beam as beam
from apache_beam.io.gcp.bigquery import BigQueryWriteFn, WriteToBigQuery
from apache_beam.options.pipeline_options import PipelineOptions, StandardOptions
from apache_beam.runners import DataflowRunner

from modules import constants, utils
from modules.transformation import ReadHarFiles, WriteBigQuery, HarJsonToSummaryDoFn


class WriteBigQuery(beam.PTransform):
    def __init__(self, known_args, standard_options, label=None):
        super().__init__(label)
        self.known_args = known_args
        self.standard_options = standard_options

    def expand(self, parsed):
        pages, requests = parsed
        requests = requests | "FlattenRequests" >> beam.FlatMap(lambda elements: elements)

        home_pages = pages | "FilterHomePages" >> beam.Filter(utils.is_home_page)
        home_requests = requests | "FilterHomeRequests" >> beam.Filter(utils.is_home_page)

        deadletter_queues = {}

        deadletter_queues["pages"] = pages | "WritePagesToBigQuery" >> WriteBigQuery(
            table=lambda row: utils.format_table_name(
                row, self.known_args.dataset_summary_pages
            ),
            schema=constants.bigquery["schemas"]["summary_pages"],
            streaming=self.standard_options.streaming,
            method=self.known_args.big_query_write_method,
        )

        deadletter_queues[
            "requests"
        ] = requests | "WriteRequestsToBigQuery" >> WriteBigQuery(
            table=lambda row: utils.format_table_name(
                row, self.known_args.dataset_summary_requests
            ),
            schema=constants.bigquery["schemas"]["summary_requests"],
            streaming=self.standard_options.streaming,
            method=self.known_args.big_query_write_method,
        )

        deadletter_queues[
            "home_pages"
        ] = home_pages | "WriteHomePagesToBigQuery" >> WriteBigQuery(
            table=lambda row: utils.format_table_name(
                row, self.known_args.dataset_summary_pages_home_only
            ),
            schema=constants.bigquery["schemas"]["summary_pages"],
            streaming=self.standard_options.streaming,
            method=self.known_args.big_query_write_method,
        )

        deadletter_queues[
            "home_requests"
        ] = home_requests | "WriteHomeRequestsToBigQuery" >> WriteBigQuery(
            table=lambda row: utils.format_table_name(
                row, self.known_args.dataset_summary_requests_home_only
            ),
            schema=constants.bigquery["schemas"]["summary_requests"],
            streaming=self.standard_options.streaming,
            method=self.known_args.big_query_write_method,
        )

        # deadletter logging
        if self.standard_options.streaming:
            for name, transform in deadletter_queues.items():
                transform_name = (
                    f"Print{utils.title_case_beam_transform_name(name)}Errors"
                )
                transform[BigQueryWriteFn.FAILED_ROWS] | transform_name >> beam.FlatMap(
                    lambda e: logging.error(f"Could not load {name} to BigQuery: {e}")
                )


class SummaryPipelineOptions(PipelineOptions):

    @classmethod
    def _add_argparse_args(cls, parser):
        super()._add_argparse_args(parser)

        parser.add_argument(
            "--input_summary",
            dest="input",
            help="Input file to process. Example: gs://httparchive/crawls/*Jan_1_2022",
        )

        parser.add_argument(
            "--subscription",
            dest="subscription",
            help="Pub/Sub subscription. Example: `projects/httparchive/subscriptions/har-gcs-pipeline`",
        )

        bq_write_methods = [
            WriteToBigQuery.Method.STREAMING_INSERTS,
            WriteToBigQuery.Method.FILE_LOADS,
        ]
        parser.add_argument(
            "--big_query_write_method",
            dest="big_query_write_method",
            help=f"BigQuery write method. One of {','.join(bq_write_methods)}",
            choices=bq_write_methods,
            default=WriteToBigQuery.Method.STREAMING_INSERTS,
        )

        parser.add_argument(
            "--dataset_summary_pages",
            dest="dataset_summary_pages",
            help="BigQuery dataset to write summary pages tables",
            default=constants.bigquery["datasets"]["summary_pages_all"],
        )

        parser.add_argument(
            "--dataset_summary_requests",
            dest="dataset_summary_requests",
            help="BigQuery dataset to write summary requests tables",
            default=constants.bigquery["datasets"]["summary_requests_all"],
        )

        parser.add_argument(
            "--dataset_summary_pages_home_only",
            dest="dataset_summary_pages_home_only",
            help="BigQuery dataset to write summary pages tables (home-page-only)",
            default=constants.bigquery["datasets"]["summary_pages_home"],
        )

        parser.add_argument(
            "--dataset_summary_requests_home_only",
            dest="dataset_summary_requests_home_only",
            help="BigQuery dataset to write summary requests tables (home-page-only)",
            default=constants.bigquery["datasets"]["summary_requests_home"],
        )


def create_pipeline(argv=None):
    parser = argparse.ArgumentParser()
    known_args, pipeline_args = parser.parse_known_args(argv)
    pipeline_options = PipelineOptions(pipeline_args, save_main_session=True)
    standard_options = pipeline_options.view_as(StandardOptions)
    known_args = pipeline_options.view_as(SummaryPipelineOptions)
    logging.info(f"Pipeline Options: {known_args=},{pipeline_args=},{pipeline_options},{standard_options}")
    if not (known_args.subscription or known_args.input):
        raise RuntimeError(
            "Either one of --input or --subscription options must be provided"
        )

    # TODO log and persist execution arguments to storage for tracking
    # https://beam.apache.org/documentation/patterns/pipeline-options/

    # TODO add metric counters for files in, processed, written to GCP & BQ

    p = beam.Pipeline(options=pipeline_options)

    (p
     | ReadHarFiles(known_args.subscription, known_args.input)
     | "ParseHar" >> beam.ParDo(HarJsonToSummaryDoFn()).with_outputs("page", "requests")
     | WriteBigQuery(known_args, standard_options)
     )

    # TODO detect DONE file, move temp table to final destination, shutdown pipeline (if streaming)

    return p


def run(argv=None):
    logging.getLogger().setLevel(logging.INFO)
    p = create_pipeline()
    pipeline_result = p.run(argv)
    if not isinstance(p.runner, DataflowRunner):
        pipeline_result.wait_until_finish()


if __name__ == '__main__':
    run()
