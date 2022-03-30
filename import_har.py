import argparse
import logging

import apache_beam as beam
from apache_beam.io import WriteToBigQuery, BigQueryDisposition
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
        '--subscription',
        dest='subscription',
        default=constants.subscription,
        help='Pub/Sub subscription'
    )
    return parser.parse_known_args(argv)


def run(argv=None):
    known_args, pipeline_args = parse_args(argv)
    pipeline_options = PipelineOptions(pipeline_args)
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

        if standard_options.streaming:
            # streaming pipeline
            big_query_params = {
                'method': WriteToBigQuery.Method.STREAMING_INSERTS,
                # 'create_disposition': BigQueryDisposition.CREATE_IF_NEEDED,
                'write_disposition': BigQueryDisposition.WRITE_APPEND,
                'triggering_frequency': 10,
                'with_auto_sharding': True,  # TODO fixed sharding instead?
                'ignore_unknown_columns ': True,  # beam v2.37.0 only valid for streaming inserts

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
                'additional_bq_parameters': {'ignoreUnknownValues': True},
                # 'schema': bigquery.SCHEMA_AUTODETECT,  # only use schema auto-detection for testing
            }

        _ = pages | 'WritePagesToBigQuery' >> WriteToBigQuery(
            table=lambda row: utils.format_table_name(row, 'pages'),
            schema=constants.big_query['schemas']['pages'],
            **big_query_params)

        _ = requests | 'WriteRequestsToBigQuery' >> WriteToBigQuery(
            table=lambda row: utils.format_table_name(row, 'requests'),
            schema=constants.big_query['schemas']['requests'],
            **big_query_params)


if __name__ == '__main__':
    logging.getLogger().setLevel(logging.INFO)
    run()
