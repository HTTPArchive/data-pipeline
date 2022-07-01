"""HTTP Archive dataflow pipeline for generating HAR data on BigQuery."""

from __future__ import absolute_import

import argparse
from copy import deepcopy
from datetime import datetime
from hashlib import sha256
import json
import logging
import re

import apache_beam as beam
from apache_beam.options.pipeline_options import PipelineOptions
from apache_beam.runners import DataflowRunner

from modules import constants, utils


# BigQuery can handle rows up to 100 MB.
MAX_CONTENT_SIZE = 10 * 1024 * 1024
# Number of times to partition the requests tables.
NUM_PARTITIONS = 4


def partition_step(function, har, client, crawl_date, index):
    """Partitions functions across multiple concurrent steps."""

    if not har:
        logging.warning('Unable to partition step, null HAR.')
        return

    page = har.get('log', {}).get('pages', [{}])[0]
    page_url = page.get('_URL')

    if not page_url:
        logging.warning('Skipping HAR: unable to get page URL (see preceding warning).')
        return

    hashed_url = hash_url(page_url)
    if hashed_url % NUM_PARTITIONS != index:
        return

    return function(har, client, crawl_date)


def get_page(har, client, crawl_date):
    """Parses the page from a HAR object."""

    if not har:
        return

    page = har.get('log').get('pages')[0]
    url = page.get('_URL')
    wptid = page.get('testID')
    date = crawl_date
    is_root_page = True
    root_page = url
    rank = None

    metadata = page.get('_metadata')
    if metadata:
        # The page URL from metadata is more accurate.
        # See https://github.com/HTTPArchive/data-pipeline/issues/48
        url = metadata.get('tested_url', url)
        client = metadata.get('layout', client).lower()
        is_root_page = metadata.get('crawl_depth', 0) == 0
        root_page = metadata.get('root_page_url', url)
        rank = int(metadata.get('rank')) if metadata.get('rank') else None

    try:
        page = trim_page(page)
        payload_json = to_json(page)
    except ValueError:
        logging.warning('Skipping pages payload for "%s": unable to stringify as JSON.', wptid)
        return

    payload_size = len(payload_json)
    if payload_size > MAX_CONTENT_SIZE:
        logging.warning('Skipping pages payload for "%s": '
                        'payload size (%s) exceeds the maximum content size of %s bytes.',
                        wptid, payload_size, MAX_CONTENT_SIZE)
        return

    custom_metrics = get_custom_metrics(page, wptid)
    lighthouse = get_lighthouse_reports(har, wptid)
    features = get_features(page, wptid)
    technologies = get_technologies(page)

    return [{
        'date': date,
        'client': client,
        'page': url,
        'is_root_page': is_root_page,
        'root_page': root_page,
        'rank': rank,
        'wptid': wptid,
        'payload': payload_json,
        # TODO: Integrate with the summary pipeline.
        'summary': '',
        'custom_metrics': custom_metrics,
        'lighthouse': lighthouse,
        'features': features,
        'technologies': technologies,
        'metadata': to_json(metadata)
    }]


def get_custom_metrics(page, wptid):
    """ Transforms the page data into a custom metrics object. """

    custom_metrics = {}

    for metric in page.get('_custom'):
        value = page.get(f'_{metric}')

        if isinstance(value, str):
            try:
                value = json.loads(value)
            except ValueError:
                logging.warning('ValueError: Unable to parse custom metric %s as JSON for %s', metric, wptid)
                continue
            except RecursionError:
                logging.warning('RecursionError: Unable to parse custom metric %s as JSON for %s', metric, wptid)
                continue

        custom_metrics[metric] = value

    try:
        custom_metrics_json = to_json(custom_metrics)
    except UnicodeEncodeError:
        logging.warning('Unable to JSON encode custom metrics for %s', wptid)
        return

    return custom_metrics_json


def get_features(page, wptid):
    """Parses the features from a page."""

    if not page:
        return

    blink_features = page.get('_blinkFeatureFirstUsed')

    if not blink_features:
        return

    def get_feature_names(feature_map, feature_type):
        feature_names = []

        try:
            for (key, value) in feature_map.items():
                if value.get('name'):
                    feature_names.append({
                        'feature': value.get('name'),
                        'type': feature_type,
                        'id': key
                    })
                    continue

                match = id_pattern.match(key)
                if match:
                    feature_names.append({
                        'feature': '',
                        'type': feature_type,
                        'id': match.group(1)
                    })
                    continue

                feature_names.append({
                    'feature': key,
                    'type': feature_type,
                    'id': ''
                })
        except ValueError:
            logging.warning('Unable to get feature names for %s. Feature_type: %s, feature_map: %s',
                            wptid, feature_type, feature_map)

        return feature_names

    id_pattern = re.compile(r'^Feature_(\d+)$')

    return get_feature_names(blink_features.get('Features'), 'default') + \
        get_feature_names(blink_features.get('CSSFeatures'), 'css') + \
        get_feature_names(blink_features.get('AnimatedCSSFeatures'), 'animated-css')


def get_technologies(page):
    """Parses the technologies from a page."""

    if not page:
        return

    app_names = page.get('_detected_apps', {})
    categories = page.get('_detected', {})

    # When there are no detected apps, it appears as an empty array.
    if isinstance(app_names, list):
        app_names = {}
        categories = {}

    technologies = {}
    app_map = {}
    for app, info_list in app_names.items():
        if not info_list:
            continue

        # There may be multiple info values. Add each to the map.
        for info in info_list.split(','):
            app_id = f'{app} {info}' if len(info) > 0 else app
            app_map[app_id] = app

    for category, apps in categories.items():
        for app_id in apps.split(','):
            app = app_map.get(app_id)
            info = ''
            if app is None:
                app = app_id
            else:
                info = app_id[len(app):].strip()

            technologies[app] = technologies.get(app, {
                'technology': app,
                'info': [],
                'categories': []
            })

            technologies.get(app).get('info').append(info)
            if category not in technologies.get(app).get('categories'):
                technologies.get(app).get('categories').append(category)

    return list(technologies.values())


def get_lighthouse_reports(har, wptid):
    """Parses Lighthouse results from a HAR object."""

    if not har:
        return

    report = har.get('_lighthouse')

    if not report:
        return

    # Omit large UGC.
    report.get('audits').get('screenshot-thumbnails', {}).get('details', {}).pop('items', None)

    try:
        report_json = to_json(report)
    except ValueError:
        logging.warning('Skipping Lighthouse report for %s: unable to stringify as JSON.', wptid)
        return

    report_size = len(report_json)
    if report_size > MAX_CONTENT_SIZE:
        logging.warning('Skipping Lighthouse report for %s: '
                        'Report size (%s) exceeded maximum content size of %s bytes.',
                        wptid, report_size, MAX_CONTENT_SIZE)
        return

    return report_json


def partition_requests(har, client, crawl_date, index):
    """Partitions requests across multiple concurrent steps."""

    if not har:
        return

    page = har.get('log').get('pages')[0]
    page_url = page.get('_URL')

    if hash_url(page_url) % NUM_PARTITIONS != index:
        return

    metadata = page.get('_metadata')
    if metadata:
        # The page URL from metadata is more accurate.
        # See https://github.com/HTTPArchive/data-pipeline/issues/48
        page_url = metadata.get('tested_url', page_url)
        client = metadata.get('layout', client).lower()

    return get_requests(har, client, crawl_date)


def get_requests(har, client, crawl_date):
    """Parses the requests from the HAR."""

    if not har:
        return

    page = har.get('log').get('pages')[0]
    page_url = page.get('_URL')
    date = crawl_date
    is_root_page = True
    root_page = page_url

    metadata = page.get('_metadata')
    if metadata:
        # The page URL from metadata is more accurate.
        # See https://github.com/HTTPArchive/data-pipeline/issues/48
        page_url = metadata.get('tested_url', page_url)
        client = metadata.get('layout', client).lower()
        is_root_page = metadata.get('crawl_depth', 0) == 0
        root_page = metadata.get('root_page_url', page_url)

    entries = har.get('log').get('entries')

    requests = []
    index = 0

    for request in entries:

        request_url = request.get('_full_url')
        is_main_document = request.get('_final_base_page', False)
        index = int(request.get('_index', index))
        request_headers = []
        response_headers = []

        index += 1
        if not request_url:
            logging.warning('Skipping empty request URL for "%s" index %s', page_url, index)
            continue

        if request.get('request') and request.get('request').get('headers'):
            request_headers = request.get('request').get('headers')

        if request.get('response') and request.get('response').get('headers'):
            response_headers = request.get('response').get('headers')

        try:
            payload = to_json(trim_request(request))
        except ValueError:
            logging.warning('Skipping requests payload for "%s": '
                            'unable to stringify as JSON.', request_url)
            continue

        payload_size = len(payload)
        if payload_size > MAX_CONTENT_SIZE:
            logging.warning('Skipping requests payload for "%s": '
                            'payload size (%s) exceeded maximum content size of %s bytes.',
                            request_url, payload_size, MAX_CONTENT_SIZE)
            continue

        response_body = None
        if request.get('response') and request.get('response').get('content'):
            response_body = request.get('response').get('content').get('text', None)

            if response_body is not None:
                response_body = response_body[:MAX_CONTENT_SIZE]

        mime_type = request.get('response').get('content').get('mimeType')
        ext = utils.get_ext(request_url)
        type = utils.pretty_type(mime_type, ext)

        requests.append({
            'date': date,
            'client': client,
            'page': page_url,
            'is_root_page': is_root_page,
            'root_page': root_page,
            'url': request_url,
            'is_main_document': is_main_document,
            'type': type,
            'index': index,
            'payload': payload,
            # TODO: Get the summary data.
            'summary': '',
            'request_headers': request_headers,
            'response_headers': response_headers,
            'response_body': response_body
        })

    return requests


def hash_url(url):
    """Hashes a given URL to a process-stable integer value."""

    return int(sha256(url.encode('utf-8')).hexdigest(), 16)


def trim_request(request):
    """Removes redundant fields from the request object."""

    if not request:
        return None

    # Make a copy first so the response body can be used later.
    request = deepcopy(request)
    request.get('response', {}).get('content', {}).pop('text', None)
    return request


def trim_page(page):
    """Removes unneeded fields from the page object."""

    if not page:
        return None

    # Make a copy first so the data can be used later.
    page = deepcopy(page)
    page.pop('_parsed_css', None)
    return page


def to_json(obj):
    """Returns a JSON representation of the object.

    This method attempts to mirror the output of the
    legacy Java Dataflow pipeline. For the most part,
    the default `json.dumps` config does the trick,
    but there are a few settings to make it more consistent:

    - Omit whitespace between properties
    - Do not escape non-ASCII characters (preserve UTF-8)

    One difference between this Python implementation and the
    Java implementation is the way long numbers are handled.
    A Python-serialized JSON string might look like this:

        "timestamp":1551686646079.9998

    while the Java-serialized string uses scientific notation:

        "timestamp":1.5516866460799998E12

    Out of a sample of 200 actual request objects, this was
    the only difference between implementations. This can be
    considered an improvement.
    """

    if not obj:
        raise ValueError

    return json.dumps(obj, separators=(',', ':'), ensure_ascii=False).encode(
        'utf-8', 'surrogatepass').decode('utf-8', 'replace')


def from_json(string):
    """Returns an object from the JSON representation."""

    try:
        return json.loads(string)
    except json.JSONDecodeError as err:
        logging.error('Unable to parse JSON object "%s...": %s', string[:50], err)
        return


def get_crawl_info(release, input_file=False):
    """Formats a release string into a BigQuery date."""

    if input_file:
        client, date_string = release.split(".")[0].split('-')
    else:
        client, date_string = release.split('/')[-1].split('-')

    if client == 'chrome':
        client = 'desktop'
    elif client == 'android':
        client = 'mobile'

    date_obj = datetime.strptime(date_string, '%b_%d_%Y')  # Mar_01_2020
    crawl_date = date_obj.strftime('%Y-%m-%d')  # 2020-03-01

    return (client, crawl_date)


def get_gcs_dir(release):
    """Formats a release string into a gs:// directory."""

    return f'gs://httparchive/{release}/'


def run(argv=None):
    """Constructs and runs the BigQuery import pipeline."""
    parser = argparse.ArgumentParser()
    group = parser.add_mutually_exclusive_group(required=True)
    group.add_argument(
        '--input',
        help='Input Cloud Storage directory to process.')
    group.add_argument(
        '--input_file',
        help="Input file containing a list of HAR files"
    )
    known_args, pipeline_args = parser.parse_known_args(argv)
    pipeline_options = PipelineOptions(pipeline_args, save_main_session=True)

    pipeline = beam.Pipeline(options=pipeline_options)

    if known_args.input:
        gcs_dir = get_gcs_dir(known_args.input)
        client, crawl_date = get_crawl_info(known_args.input)
        reader = pipeline | beam.Create([gcs_dir])
    elif known_args.input_file:
        client, crawl_date = get_crawl_info(known_args.input_file, input_file=True)
        reader = pipeline | beam.Create([known_args.input_file]) | "ReadInputFile" >> beam.io.ReadAllFromText()
    else:
        raise RuntimeError("Unexpected pipeline input path")

    hars = (
        reader
        | beam.Reshuffle()
        | "ReadHarFiles" >> beam.io.ReadAllFromText()
        | 'MapJSON' >> beam.Map(from_json)
    )

    for i in range(NUM_PARTITIONS):
        _ = (
            hars
            | f'MapPage{i}s' >> beam.FlatMap(
                (lambda i: lambda har: partition_step(get_page, har, client, crawl_date, i))(i))
            | f'WritePages{i}' >> beam.io.WriteToBigQuery(
                'httparchive:all.pages',
                schema=constants.bigquery["schemas"]["all_pages"],
                write_disposition=beam.io.BigQueryDisposition.WRITE_APPEND,
                create_disposition=beam.io.BigQueryDisposition.CREATE_NEVER))

        _ = (
            hars
            | f'MapRequests{i}' >> beam.FlatMap(
                (lambda i: lambda har: partition_step(get_requests, har, client, crawl_date, i))(i))
            | f'WriteRequests{i}' >> beam.io.WriteToBigQuery(
                'httparchive:all.requests',
                schema=constants.bigquery["schemas"]["all_requests"],
                write_disposition=beam.io.BigQueryDisposition.WRITE_APPEND,
                create_disposition=beam.io.BigQueryDisposition.CREATE_NEVER))

    pipeline_result = pipeline.run()
    if not isinstance(pipeline.runner, DataflowRunner):
        pipeline_result.wait_until_finish()
