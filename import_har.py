import argparse
import logging

import apache_beam as beam
import apache_beam.io.gcp.gcsfilesystem
from apache_beam.io import WriteToBigQuery, BigQueryDisposition
from apache_beam.io.gcp import bigquery
from apache_beam.io.gcp.bigquery_tools import FileFormat
from apache_beam.options.pipeline_options import PipelineOptions, StandardOptions

from modules import constants, utils
from modules.transformation import ImportHarJson, ReadFiles


def parse_args(argv):
    parser = argparse.ArgumentParser()
    parser.add_argument(
        '--input',
        dest='input',
        # default='gs://httparchive/experimental/input/*',
        help='Input file to process.')
    parser.add_argument(
        '--output',
        dest='output',
        # default='gs://httparchive/experimental/output',
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
    parser.add_argument(
        '--subscription',
        dest='subscription',
        default=constants.subscription,
        help='Pub/Sub subscription'
    )
    return parser.parse_known_args(argv)


def run(argv=None):
    known_args, pipeline_args = parse_args(argv)
    pipeline_options = PipelineOptions(
        pipeline_args,
        # TODO testing, move all hardcoded values below into run script
        project='httparchive',
        # staging_location='gs://httparchive/experimental/staging',
        # temp_location='gs://httparchive/experimental/temp',
        # streaming=True,
    )
    standard_options = pipeline_options.view_as(StandardOptions)
    if not (standard_options.streaming or known_args.input):
        utils.log_exeption_and_raise('Either one of --input or --streaming options must be provided')

    # TODO log and persist execution arguments to storage for tracking
    # https://beam.apache.org/documentation/patterns/pipeline-options/

    # TODO add metric counters for files in, processed, written to GCP & BQ

    with beam.Pipeline(options=pipeline_options) as p:
        parsed = (p
                  | ReadFiles(standard_options.streaming, known_args.subscription, known_args.input)
                  # | transformation.SampleFiles(sample_size=3)  # TODO only for testing, remove in prod
                  | beam.Reshuffle()  # reshuffle to unlink dependent parallelism
                  | 'ParseHar' >> beam.ParDo(ImportHarJson()).with_outputs('page', 'requests')
                  )
        pages, requests = parsed
        # TODO reshuffle? need to monitor skew
        requests = requests | 'FlattenRequests' >> beam.FlatMap(lambda elements: elements)

        pages | beam.Map(print)  # TODO remove - for debugging

        # TODO consider removing this step
        #  only used for temporary history/troubleshooting, not necessary when using WriteToBigQuery.Method.FILE_LOADS
        if known_args.output:
            _ = pages | 'WritePagesToGCS' >> beam.io.WriteToText(
                # file_path_prefix=os.path.join(known_args.output, 'page'),
                file_path_prefix=apache_beam.GCSFileSystem.join(known_args.output, 'page'),
                file_name_suffix='.jsonl')
            _ = requests | 'WriteRequestsToGCS' >> beam.io.WriteToText(
                # file_path_prefix=os.path.join(known_args.output, 'request'),
                file_path_prefix=apache_beam.GCSFileSystem(known_args.output, 'request'),
                file_name_suffix='.jsonl')

        if standard_options.streaming:
            # streaming pipeline
            big_query_params = {
                'method': WriteToBigQuery.Method.STREAMING_INSERTS,
                # 'create_disposition': BigQueryDisposition.CREATE_IF_NEEDED,
                'write_disposition': BigQueryDisposition.WRITE_APPEND,
                'triggering_frequency': 10,
                'with_auto_sharding': True,  # TODO fixed sharding instead?

                # TODO monitor streaming performance and consider switching to using FILE_LOADS
                # 'method': WriteToBigQuery.Method.FILE_LOADS,
            }
        else:
            # batch pipeline
            big_query_params = {
                'create_disposition': BigQueryDisposition.CREATE_IF_NEEDED,
                'write_disposition': BigQueryDisposition.WRITE_TRUNCATE,
                'method': WriteToBigQuery.Method.FILE_LOADS,
                'temp_file_format': FileFormat.JSON,
                'schema': bigquery.SCHEMA_AUTODETECT,
            }

        _ = pages | 'WritePagesToBigQuery' >> WriteToBigQuery(table=known_args.pages_table, **big_query_params)
        _ = requests | 'WriteRequestsToBigQuery' >> WriteToBigQuery(table=known_args.requests_table, **big_query_params)


if __name__ == '__main__':
    logging.getLogger().setLevel(logging.INFO)
    run()
