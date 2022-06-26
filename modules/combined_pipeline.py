import argparse
import logging

import apache_beam as beam
from apache_beam.io import WriteToBigQuery
from apache_beam.options.pipeline_options import PipelineOptions, StandardOptions

from modules import summary_pipeline, non_summary_pipeline, constants
from modules.transformation import ReadHarFiles, HarJsonToSummaryDoFn


class CombinedPipelineOptions(PipelineOptions):
    @classmethod
    def _add_argparse_args(cls, parser):
        super()._add_argparse_args(parser)

        parser.prog = "combined_pipeline"

        pipeline_types = ["combined", "summary", "non-summary"]
        parser.add_argument(
            "--pipeline_type",
            default="combined",
            choices=pipeline_types,
            help=f"Type of pipeline to run. One of {','.join(pipeline_types)}",
        )

        group = parser.add_mutually_exclusive_group(required=True)
        group.add_argument(
            "--input",
            dest="input",
            help="Input file to process. Example: gs://httparchive/crawls/*Jan_1_2022",
        )

        group.add_argument(
            "--subscription",
            dest="subscription",
            help="Pub/Sub subscription. Example: `projects/httparchive/subscriptions/har-gcs-pipeline`",
        )

        bq_write_methods = [
            WriteToBigQuery.Method.STREAMING_INSERTS,
            WriteToBigQuery.Method.FILE_LOADS,
        ]
        parser.add_argument(
            "--big_query_write_method_summary",
            dest="big_query_write_method",
            help=f"BigQuery write method. One of {','.join(bq_write_methods)}",
            choices=bq_write_methods,
            default=WriteToBigQuery.Method.STREAMING_INSERTS,
        )

        parser.add_argument(
            "--dataset_summary_pages",
            dest="dataset_summary_pages",
            help="BigQuery dataset to write summary pages tables",
            default=constants.BIGQUERY["datasets"]["summary_pages_all"],
        )
        parser.add_argument(
            "--dataset_summary_requests",
            dest="dataset_summary_requests",
            help="BigQuery dataset to write summary requests tables",
            default=constants.BIGQUERY["datasets"]["summary_requests_all"],
        )
        parser.add_argument(
            "--dataset_summary_pages_home_only",
            dest="dataset_summary_pages_home_only",
            help="BigQuery dataset to write summary pages tables (home-page-only)",
            default=constants.BIGQUERY["datasets"]["summary_pages_home"],
        )
        parser.add_argument(
            "--dataset_summary_requests_home_only",
            dest="dataset_summary_requests_home_only",
            help="BigQuery dataset to write summary requests tables (home-page-only)",
            default=constants.BIGQUERY["datasets"]["summary_requests_home"],
        )

        parser.add_argument(
            "--dataset_pages",
            help="BigQuery dataset to write pages table",
            default=constants.BIGQUERY["datasets"]["pages"],
        )
        parser.add_argument(
            "--dataset_technologies",
            help="BigQuery dataset to write technologies table",
            default=constants.BIGQUERY["datasets"]["technologies"],
        )
        parser.add_argument(
            "--dataset_lighthouse",
            help="BigQuery dataset to write lighthouse table",
            default=constants.BIGQUERY["datasets"]["lighthouse"],
        )
        parser.add_argument(
            "--dataset_requests",
            help="BigQuery dataset to write requests table",
            default=constants.BIGQUERY["datasets"]["requests"],
        )
        parser.add_argument(
            "--dataset_response_bodies",
            help="BigQuery dataset to write response_bodies table",
            default=constants.BIGQUERY["datasets"]["response_bodies"],
        )

        parser.add_argument(
            "--non_summary_partitions",
            dest="partitions",
            help="Number of partitions to split non-summary BigQuery write tasks",
            default=non_summary_pipeline.NUM_PARTITIONS,
        )


def create_pipeline(argv=None):
    parser = argparse.ArgumentParser()
    known_args, pipeline_args = parser.parse_known_args(argv)
    pipeline_options = PipelineOptions(pipeline_args, save_main_session=True)
    standard_options = pipeline_options.view_as(StandardOptions)
    combined_options = pipeline_options.view_as(CombinedPipelineOptions)
    logging.info(
        f"Pipeline Options: {known_args=},{pipeline_args=},{pipeline_options.get_all_options()},"
        f"{standard_options},{combined_options}"
    )

    # TODO add metric counters for files in, processed, written to GCP & BQ

    p = beam.Pipeline(options=pipeline_options)

    files = p | ReadHarFiles(combined_options.subscription, combined_options.input)

    # summary pipeline
    if combined_options.pipeline_type in ["combined", "summary"]:
        pages, requests = files | "ParseHarToSummary" >> beam.ParDo(
            HarJsonToSummaryDoFn()
        ).with_outputs("page", "requests")

        pages | summary_pipeline.WriteSummaryPagesToBigQuery(
            combined_options, standard_options
        )

        requests | summary_pipeline.WriteSummaryRequestsToBigQuery(
            combined_options, standard_options
        )

    # non-summary pipeline
    if combined_options.pipeline_type in ["combined", "non-summary"]:
        (
            files
            | "MapJSON" >> beam.MapTuple(non_summary_pipeline.from_json)
            | "AddDateAndClient" >> beam.Map(non_summary_pipeline.add_date_and_client)
            | "WriteNonSummaryTables"
            >> non_summary_pipeline.WriteNonSummaryToBigQuery(
                **combined_options.get_all_options()
            )
        )

    # TODO detect DONE file, move temp table to final destination, shutdown pipeline (if streaming)

    return p
