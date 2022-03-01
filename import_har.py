import argparse
import logging
import posixpath

import apache_beam as beam
from apache_beam.io import ReadFromText, WriteToText, WriteToBigQuery, BigQueryDisposition
from apache_beam.io.gcp import bigquery
from apache_beam.io.gcp.bigquery_tools import FileFormat
from apache_beam.options.pipeline_options import PipelineOptions

import constants
import transformation


def parse_args(argv):
    parser = argparse.ArgumentParser()
    parser.add_argument(
        '--input',
        dest='input',
        default='gs://httparchive/experimental/input/*',
        help='Input file to process.')
    parser.add_argument(
        '--output',
        dest='output',
        default='gs://httparchive/experimental/output',
        # required=True,
        help='Output file to write results to.')
    parser.add_argument(
        '--pages_table',
        dest='pages_table',
        default=constants.big_query_tables['pages'],
        help='Full path to BigQuery table for pages'
    )
    parser.add_argument(
        '--requests_table',
        dest='requests_table',
        default=constants.big_query_tables['requests'],
        help='Full path to BigQuery table for requests'
    )
    return parser.parse_known_args(argv)


def run(argv=None):
    known_args, pipeline_args = parse_args(argv)
    pipeline_options = PipelineOptions(
        pipeline_args,
        # TODO testing, move all hardcoded values below into run script
        project='httparchive',
        staging_location='gs://httparchive/experimental/staging',
        temp_location='gs://httparchive/experimental/temp',
    )

    # TODO log and persist execution arguments to storage for tracking
    # https://beam.apache.org/documentation/patterns/pipeline-options/

    # TODO add metric counters for files in, processed, written to GCP & BQ

    with beam.Pipeline(options=pipeline_options) as p:
        parsed = (p
                  | 'Read' >> ReadFromText(known_args.input)
                  # reshuffle to unlink dependent parallelism
                  # https://beam.apache.org/documentation/runtime/model/#parallelism
                  | 'Reshuffle' >> beam.Reshuffle()
                  | 'Parse' >> beam.ParDo(transformation.ImportHarJson()).with_outputs('page', 'requests')
                  )
        pages, requests = parsed
        requests = 'Flattenrequests' >> beam.FlatMap(lambda elements: elements)  # TODO reshuffle? need to monitor skew

        # pages | 'LogPages' >> beam.Map(transformation.TestLogging.log_and_apply)
        # requests | 'LogRequests' >> beam.Map(transformation.TestLogging.log_and_apply)

        # TODO consider removing this step
        #  only used for temporary history/troubleshooting, not necessary when using WriteToBigQuery.Method.FILE_LOADS
        # _ = pages | 'WritePagesToGCS' >> WriteToText(
        #     file_path_prefix=posixpath.join(known_args.output, 'page'),
        #     file_name_suffix='.jsonl')
        # _ = requests | 'WriteRequestsToGCS' >> WriteToText(
        #     file_path_prefix=posixpath.join(known_args.output, 'request'),
        #     file_name_suffix='.jsonl')

        _ = pages | 'WriteToBigQuery' >> WriteToBigQuery(
            table=known_args.pages_table,
            schema=bigquery.SCHEMA_AUTODETECT,
            create_disposition=BigQueryDisposition.CREATE_IF_NEEDED,
            write_disposition=BigQueryDisposition.WRITE_TRUNCATE,
            method=WriteToBigQuery.Method.FILE_LOADS,
            temp_file_format=FileFormat.JSON)
        _ = requests | 'WriteToBigQuery' >> WriteToBigQuery(
            table=known_args.requests_table,
            schema=bigquery.SCHEMA_AUTODETECT,
            create_disposition=BigQueryDisposition.CREATE_IF_NEEDED,
            write_disposition=BigQueryDisposition.WRITE_TRUNCATE,
            method=WriteToBigQuery.Method.FILE_LOADS,
            temp_file_format=FileFormat.JSON)


if __name__ == '__main__':
    logging.getLogger().setLevel(logging.INFO)
    run()
