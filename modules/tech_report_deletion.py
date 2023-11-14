#!/usr/bin/env python3

from sys import argv
import apache_beam as beam
from google.cloud import firestore
from apache_beam.options.pipeline_options import PipelineOptions, SetupOptions
import logging
import argparse


class QueryFirestoreDoFn(beam.DoFn):
    def __init__(self, database, project):
        self.client = None
        self.project = project
        self.database = database

    def start_bundle(self):
        # initialize client if it doesn't exist and create a collection reference
        if self.client is None:
            self.client = firestore.Client(project=self.project, database=self.database)

    def process(self, collection_name):
        collection_ref = self.client.collection(collection_name)
        docs = collection_ref.stream()

        for doc in docs:
            yield doc.id


class DeleteFromFirestoreDoFn(beam.DoFn):
    # Delete a single element from Firestore
    def __init__(self, project, database, collection):
        self.client = None
        self.project = project
        self.database = database
        self.collection = collection

    def start_bundle(self):
        # initialize client if it doesn't exist
        if self.client is None:
            self.client = firestore.Client(project=self.project, database=self.database)

    def process(self, doc_ids):
        collection_ref = self.client.collection(self.collection)
        for doc_id in doc_ids:
            timestamp = collection_ref.document(doc_id).delete()
            yield doc_id, timestamp


def parse_arguments(argv):
    """Parse command line arguments for the beam pipeline."""
    parser = argparse.ArgumentParser()

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

    # Firestore batch timeout
    parser.add_argument(
        '--batch_timeout',
        dest='batch_timeout',
        default=14400,
        help='Firestore batch timeout',
        required=False)

    # Firestore batch size
    parser.add_argument(
        '--batch_size',
        dest='batch_size',
        default=2000,
        help='Firestore batch size',
        required=False)

    # parse arguments
    known_args, pipeline_args = parser.parse_known_args(argv)
    return known_args, pipeline_args


def create_pipeline(argv=None, save_main_session=True):
    """Build the pipeline."""
    known_args, pipeline_args = parse_arguments(argv)

    pipeline_options = PipelineOptions(pipeline_args)

    # log pipeline options
    logging.info(f"Pipeline Options: {known_args=},{pipeline_args=},{pipeline_options.get_all_options()},")

    # We use the save_main_session option because one or more DoFn's in this
    # workflow rely on global context (e.g., a module imported at module level)
    pipeline_options.view_as(SetupOptions).save_main_session = save_main_session

    # with beam.Pipeline(options=pipeline_options) as p:
    p = beam.Pipeline(options=pipeline_options)

    # Read from BigQuery, convert decimal to float, group into batches, and write to Firestore
    (p
        | 'Create' >> beam.Create([known_args.firestore_collection])
        | 'QueryFirestore' >> beam.ParDo(QueryFirestoreDoFn(
            database=known_args.firestore_database,
            project=known_args.firestore_project
        ))
        | 'Batch' >> beam.BatchElements(min_batch_size=known_args.batch_size, max_batch_size=known_args.batch_size)
        | 'DeleteFromFirestore' >> beam.ParDo(DeleteFromFirestoreDoFn(
            project=known_args.firestore_project,
            database=known_args.firestore_database,
            collection=known_args.firestore_collection
        ))
        # | 'Log' >> beam.Map(logging.info)
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