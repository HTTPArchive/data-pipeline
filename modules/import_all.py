"""HTTP Archive dataflow pipeline for generating HAR data on BigQuery."""

from __future__ import absolute_import

import argparse
import json
import logging
import re
from copy import deepcopy
from functools import partial
from hashlib import sha256

import apache_beam as beam
from apache_beam.io import WriteToBigQuery
from apache_beam.options.pipeline_options import PipelineOptions

from modules import constants, utils, transformation
from modules.transformation import ReadHarFiles, add_common_pipeline_options, HarJsonToSummary

# BigQuery can handle rows up to 100 MB when using `WriteToBigQuery.Method.FILE_LOADS`
MAX_CONTENT_SIZE_100_MB = 10 * 1024 * 1024
# BigQuery can handle rows up to 100 MB when using `WriteToBigQuery.Method.STREAMING_INSERTS`
MAX_CONTENT_SIZE_10_MB = 1024 * 1024
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


def get_page(max_content_size, file_name, har):
    """Parses the page from a HAR object."""

    if not har:
        return

    date, client = utils.date_and_client_from_file_name(file_name)

    page = har.get('log').get('pages')[0]
    url = page.get('_URL')
    wptid = page.get('testID')
    date = "{:%Y-%m-%d}".format(date)
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
    if payload_size > max_content_size:
        logging.warning('Skipping pages payload for "%s": '
                        'payload size (%s) exceeds the maximum content size of %s bytes.',
                        wptid, payload_size, max_content_size)
        return

    custom_metrics = get_custom_metrics(page, wptid)
    lighthouse = get_lighthouse_reports(har, wptid, max_content_size)
    features = get_features(page, wptid)
    technologies = get_technologies(page)

    summary_page = None
    try:
        summary_page, _ = HarJsonToSummary.generate_pages(file_name, har)
        wanted_summary_fields = [field["name"] for field in constants.BIGQUERY["schemas"]["summary_pages"]["fields"]]
        summary_page = utils.dict_subset(summary_page, wanted_summary_fields)
        summary_page = json.dumps(summary_page)
    except Exception:
        logging.exception(
            f"Unable to unpack HAR, check previous logs for detailed errors. "
            f"{file_name=}, {har=}"
        )

    return [{
        'date': date,
        'client': client,
        'page': url,
        'is_root_page': is_root_page,
        'root_page': root_page,
        'rank': rank,
        'wptid': wptid,
        'payload': payload_json,
        'summary': summary_page,
        'custom_metrics': custom_metrics,
        'lighthouse': lighthouse,
        'features': features,
        'technologies': technologies,
        'metadata': to_json(metadata)
    }]


def get_custom_metrics(page, wptid):
    """ Transforms the page data into a custom metrics object. """

    page_metrics = page.get("_custom")

    if not page_metrics:
        logging.warning(f"No `_custom` attribute in page for {wptid=}")
        return None

    custom_metrics = {}

    for metric in page_metrics:
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


def get_lighthouse_reports(har, wptid, max_content_size):
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
    if report_size > max_content_size:
        logging.warning('Skipping Lighthouse report for %s: '
                        'Report size (%s) exceeded maximum content size of %s bytes.',
                        wptid, report_size, max_content_size)
        return

    return report_json


# def partition_requests(har, client, crawl_date, index):
#     """Partitions requests across multiple concurrent steps."""
#
#     if not har:
#         return
#
#     page = har.get('log').get('pages')[0]
#     page_url = page.get('_URL')
#
#     if hash_url(page_url) % NUM_PARTITIONS != index:
#         return
#
#     metadata = page.get('_metadata')
#     if metadata:
#         # The page URL from metadata is more accurate.
#         # See https://github.com/HTTPArchive/data-pipeline/issues/48
#         page_url = metadata.get('tested_url', page_url)
#         client = metadata.get('layout', client).lower()
#
#     return get_requests(har, client, crawl_date)


def get_requests(max_content_size, file_name, har):
    """Parses the requests from the HAR."""

    if not har:
        return

    date, client = utils.date_and_client_from_file_name(file_name)

    page = har.get('log').get('pages')[0]
    page_url = page.get('_URL')
    date = "{:%Y-%m-%d}".format(date)
    is_root_page = True
    root_page = page_url
    wptid = page.get('testID')

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
        if payload_size > max_content_size:
            logging.warning('Skipping requests payload for "%s": '
                            'payload size (%s) exceeded maximum content size of %s bytes.',
                            request_url, payload_size, max_content_size)
            continue

        response_body = None
        if request.get('response') and request.get('response').get('content'):
            response_body = request.get('response').get('content').get('text', None)

            if response_body is not None:
                response_body = response_body[:max_content_size]

        mime_type = request.get('response').get('content').get('mimeType')
        ext = utils.get_ext(request_url)
        type = utils.pretty_type(mime_type, ext)

        summary_request = None
        try:
            status_info = HarJsonToSummary.initialize_status_info(file_name, page)
            summary_request, _, _, _ = HarJsonToSummary.summarize_entry(request, "", "", 0, status_info)
            wanted_summary_fields = [field["name"] for field in constants.BIGQUERY["schemas"]["summary_requests"]["fields"]]
            summary_request = utils.dict_subset(summary_request, wanted_summary_fields)
            summary_request = json.dumps(summary_request)
        except Exception:
            logging.exception(
                f"Unable to unpack HAR, check previous logs for detailed errors. "
                f"{file_name=}, {har=}"
            )

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
            'summary': summary_request,
            'request_headers': request_headers,
            'response_headers': response_headers,
            'response_body': response_body,
            'wptid': wptid
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


def from_json(file_name, string):
    """Returns an object from the JSON representation."""

    try:
        return file_name, json.loads(string)
    except json.JSONDecodeError as err:
        logging.error('Unable to parse JSON object "%s...": %s', string[:50], err)
        return None, None


def log_size(typ, max_content_size, data):
    size = len(json.dumps(data).encode("utf-8"))
    if size >= max_content_size:
        logging.warning(f"Size greater than {max_content_size} bytes on {typ} for {data['wptid']}: {size}")
    return data


class AllPipelineOptions(PipelineOptions):

    @classmethod
    def _add_argparse_args(cls, parser):
        super()._add_argparse_args(parser)

        parser.prog = "all_pipeline"

        parser.add_argument(
            "--dataset_all_pages",
            help="BigQuery dataset to write all.pages table",
            default=constants.BIGQUERY["datasets"]["all_pages"],
        )
        parser.add_argument(
            "--dataset_all_requests",
            help="BigQuery dataset to write all.requests table",
            default=constants.BIGQUERY["datasets"]["all_requests"],
        )

        bq_write_methods = [
            WriteToBigQuery.Method.STREAMING_INSERTS,
            WriteToBigQuery.Method.FILE_LOADS,
        ]
        parser.add_argument(
            "--big_query_write_method",
            dest="method",
            help=f"BigQuery write method. One of {','.join(bq_write_methods)}",
            choices=bq_write_methods,
            default=WriteToBigQuery.Method.FILE_LOADS,
        )


def create_pipeline(argv=None):
    """Constructs and runs the BigQuery import pipeline."""
    parser = argparse.ArgumentParser()
    add_common_pipeline_options(parser)
    known_args, pipeline_args = parser.parse_known_args(argv)
    pipeline_options = PipelineOptions(pipeline_args, save_main_session=True)
    all_pipeline_options = pipeline_options.view_as(AllPipelineOptions)
    logging.info(
        f"Pipeline Options: {known_args=},{pipeline_args=},{pipeline_options.get_all_options()}"
    )

    if all_pipeline_options.method == WriteToBigQuery.Method.STREAMING_INSERTS:
        max_content_size = MAX_CONTENT_SIZE_10_MB
    else:
        max_content_size = MAX_CONTENT_SIZE_100_MB

    pipeline = beam.Pipeline(options=pipeline_options)

    hars = (
        pipeline
        | ReadHarFiles(**vars(known_args))
        | 'MapJSON' >> beam.MapTuple(from_json)
    )

    # TODO consider re-adding partitions
    _ = (
        hars
        | "MapPages" >> beam.FlatMapTuple(partial(get_page, max_content_size))
        | "LogPagesSize" >> beam.Map(partial(log_size, "page", max_content_size))
        | "WritePages" >> transformation.WriteBigQuery(
            table=all_pipeline_options.dataset_all_pages,
            schema=constants.BIGQUERY["schemas"]["all_pages"],
            additional_bq_parameters=constants.BIGQUERY["additional_bq_parameters"]["all_pages"],
            **all_pipeline_options.get_all_options())
        )

    _ = (
        hars
        | "MapRequests" >> beam.FlatMapTuple(partial(get_requests, max_content_size))
        | "LogRequestsSize" >> beam.Map(partial(log_size, "request", max_content_size))
        | "WriteRequests" >> transformation.WriteBigQuery(
            table=all_pipeline_options.dataset_all_requests,
            schema=constants.BIGQUERY["schemas"]["all_requests"],
            additional_bq_parameters=constants.BIGQUERY["additional_bq_parameters"]["all_requests"],
            **all_pipeline_options.get_all_options())
        )

    return pipeline
