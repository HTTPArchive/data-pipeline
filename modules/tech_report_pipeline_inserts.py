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
import hashlib
#from constants import TECHNOLOGY_QUERIES

# Inspired by https://stackoverflow.com/a/67028348

TECHNOLOGY_QUERIES = {
    "adoption": """
        CREATE TEMPORARY FUNCTION GET_ADOPTION(
            records ARRAY<STRUCT<
                client STRING,
                origins INT64
            >>
        ) RETURNS STRUCT<
            desktop INT64,
            mobile INT64
        > LANGUAGE js AS '''
        return Object.fromEntries(records.map(({client, origins}) => {
            return [client, origins];
        }));
        ''';

        SELECT
            STRING(DATE(date)) as date,
            app AS technology,
            rank,
            geo,
            GET_ADOPTION(ARRAY_AGG(STRUCT(
                client,
                origins
            ))) AS adoption
        FROM
            `httparchive.core_web_vitals.technologies`
        """,
    "lighthouse": """
    CREATE TEMPORARY FUNCTION GET_LIGHTHOUSE(
        records ARRAY<STRUCT<
            client STRING,
            median_lighthouse_score_accessibility NUMERIC,
            median_lighthouse_score_best_practices NUMERIC,
            median_lighthouse_score_performance NUMERIC,
            median_lighthouse_score_pwa NUMERIC,
            median_lighthouse_score_seo NUMERIC
    >>
    ) RETURNS ARRAY<STRUCT<
    name STRING,
    desktop STRUCT<
        median_score NUMERIC
    >,
    mobile STRUCT<
        median_score NUMERIC
    >
    >> LANGUAGE js AS '''
    const METRIC_MAP = {
        accessibility: 'median_lighthouse_score_accessibility',
        best_practices: 'median_lighthouse_score_best_practices',
        performance: 'median_lighthouse_score_performance',
        pwa: 'median_lighthouse_score_pwa',
        seo: 'median_lighthouse_score_seo',
    };

    // Initialize the Lighthouse map.
    const lighthouse = Object.fromEntries(Object.keys(METRIC_MAP).map(metricName => {
        return [metricName, {name: metricName}];
    }));

    // Populate each client record.
    records.forEach(record => {
        Object.entries(METRIC_MAP).forEach(([metricName, median_score]) => {
            lighthouse[metricName][record.client] = {median_score: record[median_score]};
        });
    });

    return Object.values(lighthouse);
    ''';

    SELECT
        STRING(DATE(date)) as date,
        app AS technology,
        rank,
        geo,
        GET_LIGHTHOUSE(ARRAY_AGG(STRUCT(
            client,
            median_lighthouse_score_accessibility,
            median_lighthouse_score_best_practices,
            median_lighthouse_score_performance,
            median_lighthouse_score_pwa,
            median_lighthouse_score_seo

        ))) AS lighthouse
    FROM
        `httparchive.core_web_vitals.technologies`
    """,
    "core_web_vitals": """
        CREATE TEMPORARY FUNCTION GET_VITALS(
            records ARRAY<STRUCT<
                client STRING,
                origins_with_good_fid INT64,
                origins_with_good_cls INT64,
                origins_with_good_lcp INT64,
                origins_with_good_fcp INT64,
                origins_with_good_ttfb INT64,
                origins_with_good_inp INT64,
                origins_with_any_fid INT64,
                origins_with_any_cls INT64,
                origins_with_any_lcp INT64,
                origins_with_any_fcp INT64,
                origins_with_any_ttfb INT64,
                origins_with_any_inp INT64,
                origins_with_good_cwv INT64,
                origins_eligible_for_cwv INT64
          >>
        ) RETURNS ARRAY<STRUCT<
            name STRING,
            desktop STRUCT<
                good_number INT64,
                tested INT64
        >,
        mobile STRUCT<
            good_number INT64,
            tested INT64
            >
        >> LANGUAGE js AS '''
        const METRIC_MAP = {
            overall: ['origins_with_good_cwv', 'origins_eligible_for_cwv'],
            LCP: ['origins_with_good_lcp', 'origins_with_any_lcp'],
            CLS: ['origins_with_good_cls', 'origins_with_any_cls'],
            FID: ['origins_with_good_fid', 'origins_with_any_fid'],
            FCP: ['origins_with_good_fcp', 'origins_with_any_fcp'],
            TTFB: ['origins_with_good_ttfb', 'origins_with_any_ttfb'],
            INP: ['origins_with_good_inp', 'origins_with_any_inp']
        };

        // Initialize the vitals map.
        const vitals = Object.fromEntries(Object.keys(METRIC_MAP).map(metricName => {
            return [metricName, {name: metricName}];
        }));

        // Populate each client record.
        records.forEach(record => {
            Object.entries(METRIC_MAP).forEach(([metricName, [good_number, tested]]) => {
                vitals[metricName][record.client] = {good_number: record[good_number], tested: record[tested]};
            });
        });

        return Object.values(vitals);
        ''';

        SELECT
            STRING(DATE(date)) as date,
            app AS technology,
            rank,
            geo,
            GET_VITALS(ARRAY_AGG(STRUCT(
                client,
                origins_with_good_fid,
                origins_with_good_cls,
                origins_with_good_lcp,
                origins_with_good_fcp,
                origins_with_good_ttfb,
                origins_with_good_inp,
                origins_with_any_fid,
                origins_with_any_cls,
                origins_with_any_lcp,
                origins_with_any_fcp,
                origins_with_any_ttfb,
                origins_with_any_inp,
                origins_with_good_cwv,
                origins_eligible_for_cwv
            ))) AS vitals
        FROM
            `httparchive.core_web_vitals.technologies`
    """,
    "technologies": """
        SELECT
            client,
            app AS technology,
            description,
            category,
            NULL AS similar_technologies,
            origins
        FROM
            `httparchive.core_web_vitals.technologies`
        JOIN
            `httparchive.core_web_vitals.technology_descriptions`
        ON
            app = technology
  """,
  "page_weight": """
        CREATE TEMPORARY FUNCTION GET_PAGE_WEIGHT(
            records ARRAY<STRUCT<
                client STRING,
                total INT64,
                js INT64,
                images INT64
            >>
        ) RETURNS ARRAY<STRUCT<
            name STRING,
            mobile STRUCT<
                median_bytes INT64
            >,
            desktop STRUCT<
                median_bytes INT64
            >
        >> LANGUAGE js AS '''
        const METRICS = ['total', 'js', 'images'];

        // Initialize the page weight map.
        const pageWeight = Object.fromEntries(METRICS.map(metricName => {
        return [metricName, {name: metricName}];
        }));

        // Populate each client record.
        records.forEach(record => {
            METRICS.forEach(metricName => {
                pageWeight[metricName][record.client] = {median_bytes: record[metricName]};
            });
        });

        return Object.values(pageWeight);
        ''';

        SELECT
            STRING(DATE(date)) as date,
            app AS technology,
            rank,
            geo,
            GET_PAGE_WEIGHT(ARRAY_AGG(STRUCT(
                client,
                median_bytes_total,
                median_bytes_js,
                median_bytes_image
            ))) AS pageWeight
        FROM
            `httparchive.core_web_vitals.technologies`
    """,
    "categories": """
        WITH categories AS (
            SELECT
                category,
                COUNT(DISTINCT root_page) AS origins
            FROM
                `httparchive.all.pages`,
                UNNEST(technologies) AS t,
                UNNEST(t.categories) AS category
            WHERE
                date = '2023-08-01' AND
                client = 'mobile'
            GROUP BY
                category
            ),

            technologies AS (
            SELECT
                category,
                technology,
                COUNT(DISTINCT root_page) AS origins
            FROM
                `httparchive.all.pages`,
                UNNEST(technologies) AS t,
                UNNEST(t.categories) AS category
            WHERE
                date = '2023-08-01' AND
                client = 'mobile'
            GROUP BY
                category,
                technology
            )

        SELECT
            category,
            categories.origins,
            ARRAY_AGG(technology ORDER BY technologies.origins DESC) AS technologies
        FROM
            categories
        JOIN
            technologies
        USING
            (category)
        GROUP BY
            category,
            categories.origins
        ORDER BY
            categories.origins DESC
  """
}

def buildQuery(start_date, end_date, query_type):
    if query_type not in TECHNOLOGY_QUERIES:
        raise ValueError(f"Query type {query_type} not found in TECHNOLOGY_QUERIES")

    query = TECHNOLOGY_QUERIES[query_type]

    if query_type != "technologies":
        # add dates to query
        if start_date and not end_date:
            query = f"{query} WHERE date >= '{start_date}'"
        elif not start_date and end_date:
            query = f"{query} WHERE date <= '{end_date}'"
        elif start_date and end_date:
            query = f"{query} WHERE date BETWEEN '{start_date}' AND '{end_date}'"
        else:
            query = query

    if query_type == "adoption" or query_type == "lighthouse" or query_type == "core_web_vitals" or query_type == "page_weight":
        query = f"{query} GROUP BY date, app, rank, geo"

    if query_type == "technologies":
        query = f"{query} WHERE date = '2023-07-01' AND geo = 'ALL' AND rank = 'ALL'"
        query = f"{query} ORDER BY origins DESC"

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

def create_hash_id(element, query_type):

    if query_type == "adoption" or query_type == "lighthouse" or query_type == "core_web_vitals" or query_type == "page_weight":
        id = (element['date'] + "-" + element['technology'] + "-" + element['geo'] + "-" + element['rank']).encode('utf-8')

    if query_type == "technologies":
        id = (element['client'] + "-" + element['technology']  + "-" + element['category']).encode('utf-8')

    if query_type == "categories":
        id = (element['category']).encode('utf-8')
        
    hash_object = hashlib.sha256(id)

    return hash_object.hexdigest()

class WriteToFirestoreBatchedDoFn(beam.DoFn):
    """Write a batch of elements to Firestore."""
    def __init__(self, database, project, collection, query_type):
        self.client = None
        self.database = database
        self.project = project
        self.collection = collection
        self.query_type = query_type

    def start_bundle(self):
        # initialize client if it doesn't exist and create a collection reference
        if self.client is None:
            self.client = firestore.Client(project=self.project, database=self.database)
            self.collection_ref = self.client.collection(self.collection)

    def process(self, elements):
        for element in elements:
            # creates a hash id for the document
            hash_id = create_hash_id(element, self.query_type)

            doc_ref = self.collection_ref.document(hash_id)
            doc_ref.set(element)

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
        | 'GroupIntoBatches' >> beam.BatchElements(min_batch_size=499, max_batch_size=499)
        | 'WriteToFirestoreBatched' >> beam.ParDo(WriteToFirestoreBatchedDoFn(
            database=known_args.firestore_database,
            project=known_args.firestore_project,
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

