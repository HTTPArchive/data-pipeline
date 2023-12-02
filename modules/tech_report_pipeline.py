#!/usr/bin/env python3

from decimal import Decimal
import hashlib
from sys import argv
import apache_beam as beam
from apache_beam.utils import retry
from google.cloud import firestore
from apache_beam.options.pipeline_options import PipelineOptions, SetupOptions
import logging
import argparse
from modules import constants

# Inspired by https://stackoverflow.com/a/67028348


def technology_hash_id(element: dict, query_type: str, key_map=constants.TECHNOLOGY_QUERY_ID_KEYS):
    """Returns a hashed id for a set of technology query keys. Keys are sorted alphabetically and joined with a dash. The resulting string is hashed using SHA256."""
    if query_type not in key_map:
        raise ValueError(f"Invalid query type: {query_type}")
    keys = sorted(key_map[query_type])
    values = [element.get(key) for key in keys]
    hash = hashlib.sha256("-".join(values).encode()).hexdigest()
    return hash


def build_query(query_type, queries=constants.TECHNOLOGY_QUERIES):
    if query_type not in queries:
        raise ValueError(f"Query type {query_type} not found in TECHNOLOGY_QUERIES")
    query = queries[query_type]
    logging.info(query)
    return query


def filter_dates(row, start_date, end_date):
    """Filter rows between start and end date"""
    if not start_date and not end_date:
        return True
    elif 'date' not in row:
        return True
    elif start_date and end_date:
        return start_date <= row['date'] <= end_date
    elif start_date:
        return start_date <= row['date']
    elif end_date:
        return row['date'] <= end_date
    else:
        return True


def convert_decimal_to_float(data):
    if isinstance(data, Decimal):
        return float(data)
    elif isinstance(data, dict):
        new_dict = {}
        for key, value in data.items():
            new_dict[key] = convert_decimal_to_float(value)
        return new_dict
    elif isinstance(data, list):
        new_list = []
        for item in data:
            new_list.append(convert_decimal_to_float(item))
        return new_list
    else:
        return data


class WriteToFirestoreDoFn(beam.DoFn):
    """Write a single element to Firestore. Yields the hash id of the document. Retry on failure using exponential backoff, see :func:`apache_beam.utils.retry.with_exponential_backoff`."""
    def __init__(self, project, database, collection, query_type):
        self.client = None
        self.project = project
        self.database = database
        self.collection = collection
        self.query_type = query_type

    def start_bundle(self):
        # initialize client if it doesn't exist and create a collection reference
        if self.client is None:
            self.client = firestore.Client(project=self.project, database=self.database)
            self.collection_ref = self.client.collection(self.collection)

    def process(self, element):
        # creates a hash id for the document
        hash_id = technology_hash_id(element, self.query_type)
        self._add_record(hash_id, element)
        yield hash_id

    @retry.with_exponential_backoff(retry_filter=lambda ex: isinstance(ex, Exception))
    def _add_record(self, id, data):
        """Helper function to add a record to Firestore. Retries on any `Exception`."""
        doc_ref = self.collection_ref.document(id)
        doc_ref.set(data)


class TechReportPipelineOptions(PipelineOptions):
    @classmethod
    def _add_argparse_args(cls, parser):
        # Query type
        parser.add_argument(
            '--query_type',
            dest='query_type',
            help='Query type',
            required=True,
            choices=constants.TECHNOLOGY_QUERIES.keys())

        # Firestore project
        parser.add_argument(
            '--firestore_project',
            dest='firestore_project',
            default='httparchive',
            help='Firestore project',
            required=True)

        # Firestore collection
        parser.add_argument(
            '--firestore_collection',
            dest='firestore_collection',
            help='Firestore collection',
            required=True)

        # Firestore database
        parser.add_argument(
            '--firestore_database',
            dest='firestore_database',
            default='(default)',
            help='Firestore database',
            required=True)

        # start date, optional
        parser.add_argument(
            '--start_date',
            dest='start_date',
            help='Start date',
            required=False)

        # end date, optional
        parser.add_argument(
            '--end_date',
            dest='end_date',
            help='End date',
            required=False)


def create_pipeline(argv=None, save_main_session=True):
    """Build the pipeline."""

    parser = argparse.ArgumentParser()
    known_args, beam_args = parser.parse_known_args()

    # Create and set your Pipeline Options.
    beam_options = PipelineOptions(beam_args)
    known_args = beam_options.view_as(TechReportPipelineOptions)

    query = build_query(known_args.query_type)

    logging.info(f"Pipeline options: {beam_options.get_all_options()}")

    # We use the save_main_session option because one or more DoFn's in this
    # workflow rely on global context (e.g., a module imported at module level)
    beam_options.view_as(SetupOptions).save_main_session = save_main_session

    p = beam.Pipeline(options=beam_options)

    # Read from BigQuery, convert decimal to float, group into batches, and write to Firestore
    (p
        | 'ReadFromBigQuery' >> beam.io.ReadFromBigQuery(query=query, use_standard_sql=True)
        | 'FilterDates' >> beam.Filter(lambda row: filter_dates(row, known_args.start_date, known_args.end_date))
        | 'ConvertDecimalToFloat' >> beam.Map(convert_decimal_to_float)
        | 'WriteToFirestore' >> beam.ParDo(WriteToFirestoreDoFn(
            project=known_args.firestore_project,
            database=known_args.firestore_database,
            collection=known_args.firestore_collection,
            query_type=known_args.query_type
        ))
    )

    return p


if __name__ == '__main__':
    logging.getLogger().setLevel(logging.INFO)
    p = create_pipeline(argv)
    logging.debug("Pipeline created")
    result = p.run()
    logging.debug("Pipeline run")

    # commented out for local testing
    # if not isinstance(p.runner, DataflowRunner):
    #     result.wait_until_finish()
