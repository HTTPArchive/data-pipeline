#!/usr/bin/env python3

from decimal import Decimal
from distutils.command import build
from sys import argv
import uuid
import apache_beam as beam
from apache_beam.runners.dataflow.dataflow_runner import DataflowRunner
from google.cloud import firestore
from apache_beam.options.pipeline_options import PipelineOptions, SetupOptions
import logging
import argparse
from constants import TECHNOLOGY_QUERIES

# Inspired by https://stackoverflow.com/a/67028348


def buildQuery(start_date, end_date, query_type):
    if query_type not in TECHNOLOGY_QUERIES:
        raise ValueError(f"Query type {query_type} not found in TECHNOLOGY_QUERIES")

    query = TECHNOLOGY_QUERIES[query_type]

    # add dates to query
    if start_date and not end_date:
        query = f"{query} WHERE date >= '{start_date}'"
    elif not start_date and end_date:
        query = f"{query} WHERE date <= '{end_date}'"
    elif start_date and end_date:
        query = f"{query} WHERE date BETWEEN '{start_date}' AND '{end_date}'"
    else:
        query = query

    # add group by to query
    query = f"{query} GROUP BY date, app, rank, geo"

    logging.info(query)

    return query


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


class WriteToFirestoreBatchedDoFn(beam.DoFn):
    """Write a batch of elements to Firestore."""
    def __init__(self, database, project, collection, batch_timeout=14400):
        self.client = None
        self.database = database
        self.project = project
        self.collection = collection
        self.batch_timeout = batch_timeout

    def start_bundle(self):
        # initialize client if it doesn't exist and create a collection reference
        if self.client is None:
            self.client = firestore.Client(project=self.project, database=self.database)
            self.collection_ref = self.client.collection(self.collection)

        # create a batch
        self.batch = self.client.batch()

    def process(self, elements):
        for element in elements:
            doc_ref = self.collection_ref.document(uuid.uuid4().hex)
            self.batch.set(doc_ref, element)

        # commit the batch with a timeout
        self.batch.commit(timeout=self.batch_timeout)


# commented out for testing later - need to compare performance between batched and individual writes
# class WriteToFirestoreDoFn(beam.DoFn):
#     """Write a single element to Firestore."""
#     def __init__(self, project, collection):
#         self.client = None
#         self.project = project
#         self.collection = collection

#     def start_bundle(self):
#         # initialize client if it doesn't exist
#         if self.client is None:
#             self.client = firestore.Client(project=self.project)

#     def process(self, element):
#         self.client.write_data(element)


def parse_arguments(argv):
    """Parse command line arguments for the beam pipeline."""
    parser = argparse.ArgumentParser()

    # Query type
    parser.add_argument(
        '--query_type',
        dest='query_type',
        help='Query type',
        required=True,
        choices=TECHNOLOGY_QUERIES.keys())

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
        default='lighthouse',
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

    # parse arguments
    known_args, pipeline_args = parser.parse_known_args(argv)
    return known_args, pipeline_args


def create_pipeline(argv=None, save_main_session=True):
    """Build the pipeline."""
    known_args, pipeline_args = parse_arguments(argv)

    query = buildQuery(known_args.start_date, known_args.end_date, known_args.query_type)

    pipeline_options = PipelineOptions(pipeline_args)

    # We use the save_main_session option because one or more DoFn's in this
    # workflow rely on global context (e.g., a module imported at module level)
    pipeline_options.view_as(SetupOptions).save_main_session = save_main_session

    # with beam.Pipeline(options=pipeline_options) as p:
    p = beam.Pipeline(options=pipeline_options)

    # Read from BigQuery, convert decimal to float, group into batches, and write to Firestore
    (p
        | 'ReadFromBigQuery' >> beam.io.ReadFromBigQuery(query=query, use_standard_sql=True)
        | 'ConvertDecimalToFloat' >> beam.Map(convert_decimal_to_float)
        | 'GroupIntoBatches' >> beam.BatchElements(min_batch_size=50, max_batch_size=50)
        | 'WriteToFirestoreBatched' >> beam.ParDo(WriteToFirestoreBatchedDoFn(
            database=known_args.firestore_database,
            project=known_args.firestore_project,
            collection=known_args.firestore_collection
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
