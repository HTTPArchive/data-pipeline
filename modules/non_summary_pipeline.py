"""HTTP Archive dataflow pipeline for generating HAR data on BigQuery."""

from __future__ import absolute_import

import argparse
import json
import logging
from copy import deepcopy
from hashlib import sha256

import apache_beam as beam
from apache_beam.io import WriteToBigQuery
from apache_beam.options.pipeline_options import PipelineOptions, StandardOptions
from apache_beam.runners import DataflowRunner

from modules import utils, constants, transformation

# BigQuery can handle rows up to 100 MB.
MAX_CONTENT_SIZE = 2 * 1024 * 1024
# Number of times to partition the requests tables.
NUM_PARTITIONS = 4


def get_page(har):
    """Parses the page from a HAR object."""

    if not har:
        return None

    page = har.get("log").get("pages")[0]
    url = page.get("_URL")

    metadata = page.get("_metadata")
    if metadata:
        # The page URL from metadata is more accurate.
        # See https://github.com/HTTPArchive/data-pipeline/issues/48
        url = metadata.get("tested_url", url)

    try:
        payload_json = to_json(page)
    except Exception:
        logging.warning(
            'Skipping pages payload for "%s": unable to stringify as JSON.' % url
        )
        return None

    payload_size = len(payload_json)
    if payload_size > MAX_CONTENT_SIZE:
        logging.warning(
            'Skipping pages payload for "%s": payload size (%s) exceeds the maximum content size of %s bytes.'
            % (url, payload_size, MAX_CONTENT_SIZE)
        )
        return None

    return [
        {
            "url": url,
            "payload": payload_json,
            "date": har["date"],
            "client": har["client"],
        }
    ]


def get_page_url(har):
    """Parses the page URL from a HAR object."""

    page = get_page(har)

    if not page:
        logging.warning("Unable to get URL from page (see preceding warning).")
        return None

    return page[0].get("url")


def partition_step(har, num_partitions):
    """Returns a partition number based on the hashed HAR page URL"""

    if not har:
        logging.warning("Unable to partition step, null HAR.")
        return 0

    page = har.get("log").get("pages")[0]
    metadata = page.get("_metadata")
    if metadata.get("crawl_depth") and metadata.get("crawl_depth") != "0":
        # Only home pages have a crawl depth of 0.
        return 0

    page_url = get_page_url(har)

    if not page_url:
        logging.warning("Skipping HAR: unable to get page URL (see preceding warning).")
        return 0

    _hash = hash_url(page_url)

    return _hash % num_partitions


def get_requests(har):
    """Parses the requests from a HAR object."""

    if not har:
        return None

    page_url = get_page_url(har)

    if not page_url:
        # The page_url field indirectly depends on the get_page function.
        # If the page data is unavailable for whatever reason, skip its requests.
        logging.warning(
            "Skipping requests payload: unable to get page URL (see preceding warning)."
        )
        return None

    entries = har.get("log").get("entries")

    requests = []

    for request in entries:

        request_url = request.get("_full_url")

        try:
            payload = to_json(trim_request(request))
        except Exception:
            logging.warning(
                'Skipping requests payload for "%s": unable to stringify as JSON.'
                % request_url
            )
            continue

        payload_size = len(payload)
        if payload_size > MAX_CONTENT_SIZE:
            logging.warning(
                'Skipping requests payload for "%s": payload size (%s) exceeded maximum content size of %s bytes.'
                % (request_url, payload_size, MAX_CONTENT_SIZE)
            )
            continue

        requests.append(
            {
                "page": page_url,
                "url": request_url,
                "payload": payload,
                "date": har["date"],
                "client": har["client"],
            }
        )

    return requests


def trim_request(request):
    """Removes redundant fields from the request object."""

    # Make a copy first so the response body can be used later.
    request = deepcopy(request)
    request.get("response").get("content").pop("text", None)
    return request


def hash_url(url):
    """Hashes a given URL to a process-stable integer value."""
    return int(sha256(url.encode("utf-8")).hexdigest(), 16)


def get_response_bodies(har):
    """Parses response bodies from a HAR object."""

    page_url = get_page_url(har)
    requests = har.get("log").get("entries")

    response_bodies = []

    for request in requests:
        request_url = request.get("_full_url")
        body = None
        if request.get("response") and request.get("response").get("content"):
            body = request.get("response").get("content").get("text", None)

        if body is None:
            continue

        truncated = len(body) > MAX_CONTENT_SIZE
        if truncated:
            logging.warning(
                'Truncating response body for "%s". Response body size %s exceeds limit %s.'
                % (request_url, len(body), MAX_CONTENT_SIZE)
            )

        response_bodies.append(
            {
                "page": page_url,
                "url": request_url,
                "body": body[:MAX_CONTENT_SIZE],
                "truncated": truncated,
                "date": har["date"],
                "client": har["client"],
            }
        )

    return response_bodies


def get_technologies(har):
    """Parses the technologies from a HAR object."""

    if not har:
        return None

    page = har.get("log").get("pages")[0]
    page_url = page.get("_URL")
    app_names = page.get("_detected_apps", {})
    categories = page.get("_detected", {})

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
                    "date": har["date"],
                    "client": har["client"],
                }
            )

    return app_list


def get_lighthouse_reports(har):
    """Parses Lighthouse results from a HAR object."""

    if not har:
        return None

    report = har.get("_lighthouse")

    if not report:
        return None

    page_url = get_page_url(har)

    if not page_url:
        logging.warning(
            "Skipping lighthouse report: unable to get page URL (see preceding warning)."
        )
        return None

    # Omit large UGC.
    report.get("audits").get("screenshot-thumbnails", {}).get("details", {}).pop(
        "items", None
    )

    try:
        report_json = to_json(report)
    except Exception:
        logging.warning(
            'Skipping Lighthouse report for "%s": unable to stringify as JSON.'
            % page_url
        )
        return None

    report_size = len(report_json)
    if report_size > MAX_CONTENT_SIZE:
        logging.warning(
            'Skipping Lighthouse report for "%s": Report size (%s) exceeded maximum content size of %s bytes.'
            % (page_url, report_size, MAX_CONTENT_SIZE)
        )
        return None

    return [
        {
            "url": page_url,
            "report": report_json,
            "date": har["date"],
            "client": har["client"],
        }
    ]


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

    return json.dumps(obj, separators=(",", ":"), ensure_ascii=False)


def from_json(file_name, element):
    """Returns an object from the JSON representation."""

    try:
        return file_name, json.loads(element)
    except Exception as e:
        logging.error('Unable to parse JSON object "%s...": %s' % (element[:50], e))
        return None


def add_date_and_client(element):
    """Adds `date` and `client` attributes to facilitate BigQuery table routing"""

    file_name, har = element
    date, client = utils.date_and_client_from_file_name(file_name)
    page = har.get("log").get("pages")[0]
    metadata = page.get("_metadata", {})
    har.update(
        {
            "date": "{:%Y_%m_%d}".format(date),
            "client": metadata.get("layout", client).lower(),
        }
    )

    return har


class WriteNonSummaryToBigQuery(beam.PTransform):
    def __init__(self, options, label=None):
        # TODO(BEAM-6158): Revert the workaround once we can pickle super() on py3.
        # super().__init__(label)
        beam.PTransform.__init__(self)
        self.non_summary_options = options.view_as(NonSummaryPipelineOptions)
        self.standard_options = options.view_as(StandardOptions)

    def _transform_and_write_partition(self, pcoll, name, index, fn, table, schema):
        return (
            pcoll
            | f"Map{utils.title_case_beam_transform_name(name)}{index}"
            >> beam.FlatMap(fn)
            | f"Write{utils.title_case_beam_transform_name(name)}{index}"
            >> transformation.WriteBigQuery(
                table=table,
                schema=schema,
                streaming=self.standard_options.streaming,
                method=self.non_summary_options.big_query_write_method,
            )
        )

    def expand(self, hars):
        partitions = hars | beam.Partition(partition_step, self.non_summary_options.partitions)

        # enumerate starting from 1, discarding the 0th elements (failures)
        for idx, part in enumerate(partitions, 1):
            self._transform_and_write_partition(
                pcoll=part,
                name="pages",
                index=idx,
                fn=get_page,
                table=lambda row: utils.format_table_name(
                    row, self.non_summary_options.dataset_pages
                ),
                schema=constants.BIGQUERY["schemas"]["pages"],
            )

            self._transform_and_write_partition(
                pcoll=part,
                name="technologies",
                index=idx,
                fn=get_technologies,
                table=lambda row: utils.format_table_name(
                    row, self.non_summary_options.dataset_technologies
                ),
                schema=constants.BIGQUERY["schemas"]["technologies"],
            )

            self._transform_and_write_partition(
                pcoll=part,
                name="lighthouse",
                index=idx,
                fn=get_lighthouse_reports,
                table=lambda row: utils.format_table_name(
                    row, self.non_summary_options.dataset_lighthouse
                ),
                schema=constants.BIGQUERY["schemas"]["lighthouse"],
            )

            self._transform_and_write_partition(
                pcoll=part,
                name="requests",
                index=idx,
                fn=get_requests,
                table=lambda row: utils.format_table_name(
                    row, self.non_summary_options.dataset_requests
                ),
                schema=constants.BIGQUERY["schemas"]["requests"],
            )

            self._transform_and_write_partition(
                pcoll=part,
                name="response_bodies",
                index=idx,
                fn=get_response_bodies,
                table=lambda row: utils.format_table_name(
                    row, self.non_summary_options.dataset_response_bodies
                ),
                schema=constants.BIGQUERY["schemas"]["response_bodies"],
            )


class NonSummaryPipelineOptions(PipelineOptions):
    @classmethod
    def _add_argparse_args(cls, parser):
        super()._add_argparse_args(parser)
        parser.add_argument(
            "--input_non_summary",
            dest="input",
            help="Input Cloud Storage directory to process.",
        )

        parser.add_argument(
            "--partitions",
            dest="partitions",
            help="Number of partitions to split BigQuery write tasks",
            default=NUM_PARTITIONS
        )

        bq_write_methods = [
            WriteToBigQuery.Method.STREAMING_INSERTS,
            WriteToBigQuery.Method.FILE_LOADS,
        ]
        parser.add_argument(
            "--big_query_write_method_non_summary",
            dest="big_query_write_method",
            help=f"BigQuery write method. One of {','.join(bq_write_methods)}",
            choices=bq_write_methods,
            default=WriteToBigQuery.Method.STREAMING_INSERTS,
        )

        parser.add_argument(
            "--dataset_pages",
            help="BigQuery dataset to write pages table",
            default=constants.BIGQUERY["datasets"]["pages"],
        )
        parser.add_argument(
            "--dataset_technologies",
            help="BigQuery dataset to write technologies table",
            default=constants.BIGQUERY["datasets"]["technologies"],
        )
        parser.add_argument(
            "--dataset_lighthouse",
            help="BigQuery dataset to write lighthouse table",
            default=constants.BIGQUERY["datasets"]["lighthouse"],
        )
        parser.add_argument(
            "--dataset_requests",
            help="BigQuery dataset to write requests table",
            default=constants.BIGQUERY["datasets"]["requests"],
        )
        parser.add_argument(
            "--dataset_response_bodies",
            help="BigQuery dataset to write response_bodies table",
            default=constants.BIGQUERY["datasets"]["response_bodies"],
        )


def create_pipeline(argv=None):
    """Constructs and runs the BigQuery import pipeline."""
    parser = argparse.ArgumentParser()
    known_args, pipeline_args = parser.parse_known_args(argv)
    pipeline_options = PipelineOptions(pipeline_args, save_main_session=True)
    known_args = pipeline_options.view_as(NonSummaryPipelineOptions)
    if pipeline_options.view_as(StandardOptions).streaming:
        raise NotImplementedError(
            "Unable to run non-summary pipeline in streaming mode, please use batch instead"
        )

    p = beam.Pipeline(options=pipeline_options)

    (
        p
        | beam.Create([known_args.input])
        | beam.io.ReadAllFromText(with_filename=True)
        | "MapJSON" >> beam.MapTuple(from_json)
        | "AddDateAndClient" >> beam.Map(add_date_and_client)
        | WriteNonSummaryToBigQuery(pipeline_options)
    )

    return p


def run(argv=None):
    logging.getLogger().setLevel(logging.INFO)
    p = create_pipeline()
    pipeline_result = p.run(argv)
    if not isinstance(p.runner, DataflowRunner):
        pipeline_result.wait_until_finish()


if __name__ == "__main__":
    run()
