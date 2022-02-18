import argparse
import logging
import posixpath

import apache_beam as beam
from apache_beam.io import ReadFromText, WriteToText, WriteToBigQuery, BigQueryDisposition
from apache_beam.io.gcp import bigquery
from apache_beam.io.gcp.bigquery_tools import FileFormat
from apache_beam.options.pipeline_options import PipelineOptions

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
        help='Output file to write results to.'),
    return parser.parse_known_args(argv)


def run(argv=None):
    known_args, pipeline_args = parse_args(argv)
    pipeline_options = PipelineOptions(
        pipeline_args,
        project='httparchive',
        staging_location='gs://httparchive/experimental/staging',
        temp_location='gs://httparchive/experimental/temp'
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
                  # | 'Parse' >> beam.Map(transformation.generate_pages)
                  | 'Parse' >> beam.ParDo(transformation.ImportHarJson())
                  | 'Log1' >> beam.Map(transformation.log_and_apply)
                  )

        _ = parsed | 'Write' >> WriteToText(
            file_path_prefix=posixpath.join(known_args.output, 'page'),
            file_name_suffix='.jsonl')

        _ = parsed | 'Load' >> WriteToBigQuery(
                    table='page',
                    dataset='experimental',
                    schema=bigquery.SCHEMA_AUTODETECT,
                    create_disposition=BigQueryDisposition.CREATE_IF_NEEDED,
                    write_disposition=BigQueryDisposition.WRITE_TRUNCATE,
                    method=WriteToBigQuery.Method.FILE_LOADS,
                    temp_file_format=FileFormat.JSON)


if __name__ == '__main__':
    logging.getLogger().setLevel(logging.INFO)
    run()
