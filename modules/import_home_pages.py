"""HTTP Archive dataflow pipeline for generating HAR data on BigQuery."""

from __future__ import absolute_import

import argparse
import json
import logging
import re
from copy import deepcopy
from functools import partial
from hashlib import sha256
from typing import Union, Dict

import apache_beam as beam
from apache_beam.options.pipeline_options import PipelineOptions

from modules import constants, utils, transformation
from modules.transformation import (
    ReadHarFiles,
    add_common_pipeline_options,
    HarJsonToSummary,
)


def get_metadata(har):

    if not har:
        return None

    page = har.get("log").get("pages")[0]

    if not page:
        return None

    metadata = page.get("_metadata")
    return metadata


def is_home_page(mapped_har):
    if not mapped_har:
        return False
    metadata = get_metadata(mapped_har)
    if metadata:
        # use metadata.crawl_depth starting from 2022-05
        if isinstance(metadata, dict):
            return metadata.get("crawl_depth", 0) == 0
        else:
            return json.loads(metadata).get("crawl_depth", 0) == 0
    else:
        # legacy crawl data is all home-page only (i.e. no secondary pages)
        return True


def get_page(max_content_size, file_name, har):
    """Parses the page from a HAR object."""

    if not har:
        return None

    if not is_home_page(har):
        return None

    date, client = utils.date_and_client_from_file_name(file_name)

    page = har.get("log").get("pages")[0]
    url = page.get("_URL")
    date = "{:%Y_%m_%d}".format(date)
    wptid = page.get("testID")

    metadata = get_metadata(har)
    if metadata:
        # The page URL from metadata is more accurate.
        # See https://github.com/HTTPArchive/data-pipeline/issues/48
        url = metadata.get("tested_url", url)
        client = metadata.get("layout", client).lower()

    try:
        page = trim_page(page)
        payload_json = to_json(page)
    except ValueError:
        logging.warning(
            'Skipping pages payload for "%s": unable to stringify as JSON.', wptid
        )
        return None

    ret = {
        "url": url,
        "payload": payload_json,
        "date": date,
        "client": client,
        "metadata": metadata,
    }

    if json_exceeds_max_content_size(ret, max_content_size, "pages", wptid):
        return None

    return [ret]


def get_parsed_css(max_content_size, file_name, har):
    """Extracts the parsed CSS custom metric from the HAR."""

    if not har:
        return None

    if not is_home_page(har):
        return None

    date, client = utils.date_and_client_from_file_name(file_name)

    page = har.get("log").get("pages")[0]
    page_url = page.get("_URL")
    date = "{:%Y_%m_%d}".format(date)
    wptid = page.get("testID")
    is_root_page = True

    metadata = get_metadata(har)
    if metadata:
        # The page URL from metadata is more accurate.
        # See https://github.com/HTTPArchive/data-pipeline/issues/48
        page_url = metadata.get("tested_url", page_url)
        client = metadata.get("layout", client).lower()
        is_root_page = metadata.get("crawl_depth", 0) == 0

    if not page:
        return None

    if not page_url:
        logging.warning("Skipping parsed CSS, no page URL")
        return None

    custom_metric = page.get("_parsed_css")

    if not custom_metric:
        logging.warning("No parsed CSS data for page %s", page_url)
        return None

    parsed_css = []

    for entry in custom_metric:
        url = entry.get("url")
        ast = entry.get("ast")

        if url == 'inline':
            # Skip inline styles for now. They're special.
            continue

        try:
            ast_json = to_json(ast)
        except Exception:
            logging.warning(
                'Unable to stringify parsed CSS to JSON for "%s".'
                % url
            )
            continue

        if json_exceeds_max_content_size(ast_json, max_content_size, "parsed_css", wptid):
            continue

        parsed_css.append({
            "date": date,
            "client": client,
            "page": page_url,
            "is_root_page": is_root_page,
            "url": url,
            "css": ast_json
        })

    return parsed_css


def get_lighthouse(max_content_size, file_name, har):
    """Parses Lighthouse results from a HAR object."""

    if not har:
        return None

    if not is_home_page(har):
        return None

    date, client = utils.date_and_client_from_file_name(file_name)

    page = har.get("log").get("pages")[0]
    url = page.get("_URL")
    date = "{:%Y_%m_%d}".format(date)
    wptid = page.get("testID")

    report = har.get("_lighthouse")
    if not report:
        return None

    metadata = get_metadata(har)
    if metadata:
        # The page URL from metadata is more accurate.
        # See https://github.com/HTTPArchive/data-pipeline/issues/48
        url = metadata.get("tested_url", url)
        client = metadata.get("layout", client).lower()

    # Omit large UGC.
    if report.get("audits") and report.get("audits").get("screenshot-thumbnails", {}) and report.get("audits").get("screenshot-thumbnails", {}).get("details", {}):
        report.get("audits").get("screenshot-thumbnails", {}).get("details", {}).pop(
            "items", None
        )

    try:
        report_json = to_json(report)
    except ValueError:
        logging.warning(
            'Skipping pages payload for "%s": unable to stringify as JSON.', wptid
        )
        return None

    ret = {
        "url": url,
        "report": report_json,
        "date": date,
        "client": client,
        "metadata": metadata,
    }

    if json_exceeds_max_content_size(ret, max_content_size, "lighthouse", wptid):
        return None

    return [ret]


def get_summary_pages(max_content_size, file_name, har):
    """Parses the sumary page from a HAR object."""

    if not har:
        return None

    if not is_home_page(har):
        return None

    date, client = utils.date_and_client_from_file_name(file_name)

    page = har.get("log").get("pages")[0]
    url = page.get("_URL")
    date = "{:%Y_%m_%d}".format(date)
    wptid = page.get("testID")

    metadata = get_metadata(har)
    if metadata:
        # The page URL from metadata is more accurate.
        # See https://github.com/HTTPArchive/data-pipeline/issues/48
        url = metadata.get("tested_url", url)
        client = metadata.get("layout", client).lower()

    summary_page = None
    try:
        summary_page, _ = HarJsonToSummary.generate_pages(file_name, har)
        wanted_summary_fields = [
            field["name"]
            for field in constants.BIGQUERY["schemas"]["summary_pages"]["fields"]
        ]

        # Needed for the table name
        wanted_summary_fields.append("date")
        wanted_summary_fields.append("client")

        summary_page = utils.dict_subset(summary_page, wanted_summary_fields)
    except Exception:
        logging.exception(
            f"Unable to unpack HAR, check previous logs for detailed errors. "
            f"{file_name=}, {har=}"
        )

    return [summary_page]


def get_summary_requests(max_content_size, file_name, har):
    """Parses the summary request from a HAR object."""

    if not har:
        return None

    if not is_home_page(har):
        return None

    date, client = utils.date_and_client_from_file_name(file_name)

    page = har.get("log").get("pages")[0]
    url = page.get("_URL")
    date = "{:%Y_%m_%d}".format(date)
    wptid = page.get("testID")

    metadata = get_metadata(har)
    if metadata:
        # The page URL from metadata is more accurate.
        # See https://github.com/HTTPArchive/data-pipeline/issues/48
        url = metadata.get("tested_url", url)
        client = metadata.get("layout", client).lower()

    try:
        _, requests = HarJsonToSummary.generate_pages(file_name, har)
    except Exception:
        logging.exception(
            f"Unable to unpack HAR, check previous logs for detailed errors. "
            f"{file_name=}, {har=}"
        )
        return None

    summary_requests = []

    for request in requests:

        try:
            wanted_summary_fields = [
                field["name"]
                for field in constants.BIGQUERY["schemas"]["summary_requests"]["fields"]
            ]

            # Needed for the table name
            wanted_summary_fields.append("date")
            wanted_summary_fields.append("client")

            request = utils.dict_subset(request, wanted_summary_fields)
        except Exception:
            logging.exception(
                f"Unable to unpack HAR, check previous logs for detailed errors. "
                f"{file_name=}, {har=}"
            )
            continue

        if request:
            summary_requests.append(request)

    return summary_requests


def get_technologies(max_content_size, file_name, har):
    """Parses the technologies from a HAR object."""

    if not har:
        return None

    if not is_home_page(har):
        return None

    date, client = utils.date_and_client_from_file_name(file_name)

    page = har.get("log").get("pages")[0]
    page_url = page.get("_URL")
    date = "{:%Y_%m_%d}".format(date)
    wptid = page.get("testID")

    metadata = get_metadata(har)
    if metadata:
        # The page URL from metadata is more accurate.
        # See https://github.com/HTTPArchive/data-pipeline/issues/48
        page_url = metadata.get("tested_url", page_url)
        client = metadata.get("layout", client).lower()

    is_root_page = True
    if metadata:
        is_root_page = metadata.get("crawl_depth") == 0

    if not page:
        return None

    if not page_url:
        logging.warning("Skipping technologies, no page URL")
        return None

    app_names = page.get("_detected_apps", {})
    categories = page.get("_detected", {})
    metadata = get_metadata(har)

    # When there are no detected apps, it appears as an empty array.
    if isinstance(app_names, list):
        app_names = {}
        categories = {}

    app_map = {}
    app_list = []
    for app, info_list in app_names.items():
        if not info_list:
            continue
        # There may be multiple info values. Add each to the map.
        for info in info_list.split(","):
            app_id = "%s %s" % (app, info) if len(info) > 0 else app
            app_map[app_id] = app

    for category, apps in categories.items():
        for app_id in apps.split(","):
            app = app_map.get(app_id)
            info = ""
            if app is None:
                app = app_id
            else:
                info = app_id[len(app):].strip()

            app_list.append(
                {
                    "url": page_url,
                    "category": category,
                    "app": app,
                    "info": info,
                    "date": date,
                    "client": client,
                    "metadata": metadata,
                }
            )

    return app_list


def get_requests(max_content_size, file_name, har):
    """Parses the requests from the HAR."""

    if not har:
        return None

    if not is_home_page(har):
        return None

    date, client = utils.date_and_client_from_file_name(file_name)

    page = har.get("log").get("pages")[0]
    page_url = page.get("_URL")
    date = "{:%Y_%m_%d}".format(date)
    wptid = page.get("testID")

    entries = har.get("log").get("entries")

    requests = []

    for request in entries:

        request_url = request.get("_full_url")

        try:
            payload = to_json(trim_request(request))
        except ValueError:
            logging.warning(
                'Skipping requests payload for "%s": ' "unable to stringify as JSON.",
                request_url,
            )
            continue

        metadata = get_metadata(har)

        ret = {
            "page": page_url,
            "url": request_url,
            "payload": payload,
            "date": date,
            "client": client,
            "metadata": metadata,
        }

        if json_exceeds_max_content_size(ret, max_content_size, "requests", wptid):
            continue

        requests.append(ret)

    return requests


def get_response_bodies(max_content_size, file_name, har):
    """Parses the requests from the HAR."""

    if not har:
        return None

    if not is_home_page(har):
        return None

    date, client = utils.date_and_client_from_file_name(file_name)

    page = har.get("log").get("pages")[0]
    page_url = page.get("_URL")
    date = "{:%Y_%m_%d}".format(date)
    wptid = page.get("testID")

    entries = har.get("log").get("entries")

    requests = []

    for request in entries:

        request_url = request.get("_full_url")
        response_body = None
        if request.get("response") and request.get("response").get("content"):
            response_body = request.get("response").get("content").get("text", None)

        if response_body is None:
            continue

        truncated = len(response_body) > constants.MaxContentSize.RESPONSE_BODIES.value
        if truncated:
            logging.warning(
                'Truncating response body for "%s". Response body size %s exceeds limit %s.'
                % (request_url, len(response_body), constants.MaxContentSize.RESPONSE_BODIES.value)
            )

        response_body = response_body[:constants.MaxContentSize.RESPONSE_BODIES.value]

        metadata = get_metadata(har)

        ret = {
            "page": page_url,
            "url": request_url,
            "body": response_body,
            "truncated": truncated,
            "date": date,
            "client": client,
            "metadata": metadata,
        }

        if json_exceeds_max_content_size(ret, max_content_size, "response_bodies", wptid):
            continue

        requests.append(ret)

    return requests


def trim_request(request):
    """Removes redundant fields from the request object."""

    if not request:
        return None

    # Make a copy first so the response body can be used later.
    request = deepcopy(request)
    request.get("response", {}).get("content", {}).pop("text", None)
    return request


def trim_page(page):
    """Removes unneeded fields from the page object."""

    if not page:
        return None

    # Make a copy first so the data can be used later.
    page = deepcopy(page)
    page.pop("_parsed_css", None)
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

    return (
        json.dumps(obj, separators=(",", ":"), ensure_ascii=False)
        .encode("utf-8", "surrogatepass")
        .decode("utf-8", "replace")
    )


def from_json(file_name, string):
    """Returns an object from the JSON representation."""

    try:
        return [(file_name, json.loads(string))]
    except json.JSONDecodeError as err:
        logging.error('Unable to parse file %s into JSON object "%s...": %s' % (file_name, string[:50], err))
        return None


def json_exceeds_max_content_size(data: Union[Dict, str], max_content_size, typ, wptid):
    if isinstance(data, dict):
        payload = to_json(data)
    else:
        payload = data

    size = len(payload.encode("utf-8"))
    oversized = size > max_content_size
    if oversized:
        logging.warning(
            f'Skipping {typ} payload for "{wptid}": '
            f"payload size ({size}) exceeds the maximum content size of {max_content_size} bytes."
        )
    return oversized


class HomePagesPipelineOptions(PipelineOptions):
    @classmethod
    def _add_argparse_args(cls, parser):
        super()._add_argparse_args(parser)

        parser.prog = "home_pages_pipeline"

        parser.add_argument(
            "--dataset_lighthouse_home_only",
            help="BigQuery dataset to write lighthouse table (home-page-only)",
            default=constants.BIGQUERY["datasets"]["lighthouse_home"],
        )
        parser.add_argument(
            "--dataset_pages_home_only",
            help="BigQuery dataset to write pages table (home-page-only)",
            default=constants.BIGQUERY["datasets"]["pages_home"],
        )
        parser.add_argument(
            "--dataset_parsed_css_home_only",
            help="BigQuery dataset to write parsed_css table (home-page-only)",
            default=constants.BIGQUERY["datasets"]["parsed_css_home"],
        )
        parser.add_argument(
            "--dataset_requests_home_only",
            help="BigQuery dataset to write requests table (home-page-only)",
            default=constants.BIGQUERY["datasets"]["requests_home"],
        )
        parser.add_argument(
            "--dataset_response_bodies_home_only",
            help="BigQuery dataset to write response_bodies table (home-page-only)",
            default=constants.BIGQUERY["datasets"]["response_bodies_home"],
        )
        parser.add_argument(
            "--dataset_summary_pages_home_only",
            help="BigQuery dataset to write summary pages tables (home-page-only)",
            default=constants.BIGQUERY["datasets"]["summary_pages_home"],
        )
        parser.add_argument(
            "--dataset_summary_requests_home_only",
            help="BigQuery dataset to write summary requests tables (home-page-only)",
            default=constants.BIGQUERY["datasets"]["summary_requests_home"],
        )
        parser.add_argument(
            "--dataset_technologies_home_only",
            help="BigQuery dataset to write technologies table (home-page-only)",
            default=constants.BIGQUERY["datasets"]["technologies_home"],
        )


def create_pipeline(argv=None):
    """Constructs and runs the BigQuery import pipeline."""
    parser = argparse.ArgumentParser()
    add_common_pipeline_options(parser)
    known_args, pipeline_args = parser.parse_known_args(argv)
    pipeline_options = PipelineOptions(pipeline_args, save_main_session=True)
    home_pages_pipeline_options = pipeline_options.view_as(HomePagesPipelineOptions)
    logging.info(
        f"Pipeline Options: {known_args=},{pipeline_args=},{pipeline_options.get_all_options()}"
    )

    max_content_size = constants.MaxContentSize.FILE_LOADS.value

    pipeline = beam.Pipeline(options=pipeline_options)

    hars = (
        pipeline
        | ReadHarFiles(**vars(known_args))
        | "MapJSON" >> beam.FlatMapTuple(from_json)
    )

    _ = (
        hars
        | "MapLighthouse" >> beam.FlatMapTuple(partial(get_lighthouse, max_content_size))
        | "WriteLighthouse" >> transformation.WriteBigQuery(
            table=lambda row: utils.format_table_name(row, home_pages_pipeline_options.dataset_lighthouse_home_only),
            schema=constants.BIGQUERY["schemas"]["lighthouse"],
            additional_bq_parameters=constants.BIGQUERY["additional_bq_parameters"]["lighthouse_home"],
            **home_pages_pipeline_options.get_all_options(),
        )
    )

    _ = (
        hars
        | "MapPages" >> beam.FlatMapTuple(partial(get_page, max_content_size))
        | "WritePages" >> transformation.WriteBigQuery(
            table=lambda row: utils.format_table_name(row, home_pages_pipeline_options.dataset_pages_home_only),
            schema=constants.BIGQUERY["schemas"]["pages"],
            additional_bq_parameters=constants.BIGQUERY["additional_bq_parameters"]["pages_home"],
            **home_pages_pipeline_options.get_all_options(),
        )
    )

    _ = (
        hars
        | "MapParsedCSS" >> beam.FlatMapTuple(partial(get_parsed_css, max_content_size))
        | "WriteParsedCSS" >> transformation.WriteBigQuery(
            table=lambda row: utils.format_table_name(row, home_pages_pipeline_options.dataset_parsed_css_home_only),
            schema=constants.BIGQUERY["schemas"]["parsed_css"],
            additional_bq_parameters=constants.BIGQUERY["additional_bq_parameters"]["parsed_css_home"],
            **home_pages_pipeline_options.get_all_options(),
        )
    )

    _ = (
        hars
        | "MapRequests" >> beam.FlatMapTuple(partial(get_requests, max_content_size))
        | "WriteRequests" >> transformation.WriteBigQuery(
            table=lambda row: utils.format_table_name(row, home_pages_pipeline_options.dataset_requests_home_only),
            schema=constants.BIGQUERY["schemas"]["requests"],
            additional_bq_parameters=constants.BIGQUERY["additional_bq_parameters"]["requests_home"],
            **home_pages_pipeline_options.get_all_options(),
        )
    )

    _ = (
        hars
        | "MapResponseBodies" >> beam.FlatMapTuple(partial(get_response_bodies, max_content_size))
        | "WriteResponseBodies" >> transformation.WriteBigQuery(
            table=lambda row: utils.format_table_name(row, home_pages_pipeline_options.dataset_response_bodies_home_only),
            schema=constants.BIGQUERY["schemas"]["response_bodies"],
            additional_bq_parameters=constants.BIGQUERY["additional_bq_parameters"]["response_bodies_home"],
            **home_pages_pipeline_options.get_all_options(),
        )
    )

    _ = (
        hars
        | "MapSummaryPages" >> beam.FlatMapTuple(partial(get_summary_pages, max_content_size))
        | "WriteSummaryPages" >> transformation.WriteBigQuery(
            table=lambda row: utils.format_table_name(row, home_pages_pipeline_options.dataset_summary_pages_home_only),
            schema=constants.BIGQUERY["schemas"]["summary_pages"],
            additional_bq_parameters=constants.BIGQUERY["additional_bq_parameters"]["summary_pages_home"],
            **home_pages_pipeline_options.get_all_options(),
        )
    )

    _ = (
        hars
        | "MapSummaryRequests" >> beam.FlatMapTuple(partial(get_summary_requests, max_content_size))
        | "WriteSummaryRequests" >> transformation.WriteBigQuery(
            table=lambda row: utils.format_table_name(row, home_pages_pipeline_options.dataset_summary_requests_home_only),
            schema=constants.BIGQUERY["schemas"]["summary_requests"],
            additional_bq_parameters=constants.BIGQUERY["additional_bq_parameters"]["summary_requests_home"],
            **home_pages_pipeline_options.get_all_options(),
        )
    )

    _ = (
        hars
        | "MapTechnologies" >> beam.FlatMapTuple(partial(get_technologies, max_content_size))
        | "WriteTechnologies" >> transformation.WriteBigQuery(
            table=lambda row: utils.format_table_name(row, home_pages_pipeline_options.dataset_technologies_home_only),
            schema=constants.BIGQUERY["schemas"]["technologies"],
            additional_bq_parameters=constants.BIGQUERY["additional_bq_parameters"]["technologies_home"],
            **home_pages_pipeline_options.get_all_options(),
        )
    )

    return pipeline
