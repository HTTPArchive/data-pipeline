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
    pipeline_options = PipelineOptions(pipeline_args, save_main_session=True)
    standard_options = pipeline_options.view_as(StandardOptions)
    if not (standard_options.streaming or known_args.input):
        raise RuntimeError('Either one of --input or --streaming options must be provided')

    # TODO log and persist execution arguments to storage for tracking
    # https://beam.apache.org/documentation/patterns/pipeline-options/

    # TODO add metric counters for files in, processed, written to GCP & BQ

    with beam.Pipeline(options=pipeline_options) as p:
        parsed = (p
                  | ReadFiles(standard_options.streaming, known_args.subscription, known_args.input)
                  | 'ParseHar' >> beam.ParDo(ImportHarJson()).with_outputs('page', 'requests')
                  )
        pages, requests = parsed
        requests = requests | 'FlattenRequests' >> beam.FlatMap(lambda elements: elements)

        if standard_options.streaming:
            # streaming pipeline
            big_query_params = {
                'create_disposition': BigQueryDisposition.CREATE_IF_NEEDED,
                'write_disposition': BigQueryDisposition.WRITE_APPEND,
                'triggering_frequency': 5 * 60,  # seconds
                'with_auto_sharding': True,

                # TODO try streaming again when `ignore_unknown_columns` works
                #  `ignore_unknown_columns` broken for `STREAMING_INSERTS` until BEAM-14039 fix is released
                #   https://issues.apache.org/jira/browse/BEAM-14039
                #   https://github.com/apache/beam/pull/16999
                # 'method': WriteToBigQuery.Method.STREAMING_INSERTS,
                # 'ignore_unknown_columns': True,
                # 'insert_retry_strategy': RetryStrategy.RETRY_ON_TRANSIENT_ERROR,

                'method': WriteToBigQuery.Method.FILE_LOADS,
                'additional_bq_parameters': {'ignoreUnknownValues': True},
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

        pages_result = pages | 'WritePagesToBigQuery' >> WriteToBigQuery(
            table=lambda row: utils.format_table_name(row, 'pages'),
            schema=constants.big_query['schemas']['pages'],
            **big_query_params)

        requests_result = requests | 'WriteRequestsToBigQuery' >> WriteToBigQuery(
            table=lambda row: utils.format_table_name(row, 'requests'),
            schema=constants.big_query['schemas']['requests'],
            **big_query_params)

        # TODO FAILED_ROWS not implemented for BigQueryBatchFileLoads in this version of beam (only _StreamToBigQuery)
        # (pages_result[BigQueryWriteFn.FAILED_ROWS]
        #  | 'PrintPagesErrors' >> beam.FlatMap(lambda e: logging.error(f'Could not load page to BigQuery: {e}')))
        #
        # (requests_result[BigQueryWriteFn.FAILED_ROWS]
        #  | 'PrintRequestsErrors' >> beam.FlatMap(lambda e: logging.error(f'Could not load request to BigQuery: {e}')))

        # pages_result[BigQueryBatchFileLoads.DESTINATION_JOBID_PAIRS] | 'printjobid' >> beam.Map(lambda e: print(f"jobidpair: {e}"))
        # pages_result[BigQueryBatchFileLoads.DESTINATION_FILE_PAIRS] | 'printfiles' >> beam.Map(lambda e: print(f"files: {e}"))
        # pages_result[BigQueryBatchFileLoads.DESTINATION_COPY_JOBID_PAIRS] | 'printcopies' >> beam.Map(lambda e: print(f"copies: {e}"))
        # TODO detect DONE file, move temp table to final destination, shutdown pipeline (if streaming)


if __name__ == '__main__':
    logging.getLogger().setLevel(logging.INFO)
    run()
