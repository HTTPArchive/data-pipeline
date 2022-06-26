"""HTTP Archive dataflow pipeline for generating HAR data on BigQuery."""

from __future__ import absolute_import

import argparse
from hashlib import sha256
import json
import logging

import apache_beam as beam
import apache_beam.io.gcp.gcsio as gcsio
from apache_beam.options.pipeline_options import PipelineOptions
from apache_beam.options.pipeline_options import SetupOptions
from apache_beam.runners import DataflowRunner

from modules import utils
from modules.css_parser import CSSParser


# BigQuery can handle rows up to 100 MB.
MAX_CONTENT_SIZE = 2 * 1024 * 1024
# Number of times to partition the requests tables.
NUM_PARTITIONS = 4


def get_parsed_css(har):
    """Returns a list of stylesheets contained in the HAR."""

    if not har:
        return None

    page = har.get('log').get('pages')[0]
    page_url = page.get('_URL')
    metadata = page.get('_metadata')
    is_home_page = utils.is_home_page(har)
    if metadata:
        page_url = metadata.get('tested_url', page_url)

    entries = har.get('log').get('entries')

    parsed_css = []

    for request in entries:
        request_url = request.get('_full_url')
        response = request.get('response').get('content')
        mime_type = response.get('mimeType')
        ext = utils.get_ext(request_url)
        typ = utils.pretty_type(mime_type, ext)

        if typ != 'css':
            continue

        parsing_options = {
            'silent': True
        }

        response_body = response.get('text')

        if not response_body:
            continue

        try:
            parser = CSSParser(response_body, parsing_options)
            ast = parser.parse()
            css = json.dumps(ast)
        except Exception as e:
            logging.error('Unable to parse CSS at "%s": %s' % (request_url, e))
            continue

        if len(css) > MAX_CONTENT_SIZE:
            logging.warning('Parsed CSS at "%s" exceeds max content size: %s' % (request_url, len(css)))
            continue

        parsed_css.append({
            'page': page_url,
            'is_home_page': is_home_page,
            'url': request_url,
            'css': css
        })

    return parsed_css


def get_page_url(har):
    """Parses the page URL from a HAR object."""

    page = har.get('log').get('pages')[0]
    if not page:
        logging.warning('Unable to get URL from page (see preceding warning).')
        return None

    return page.get('_URL')


def partition_step(fn, har, index):
    """Partitions functions across multiple concurrent steps."""

    logging.info(f'partitioning step {fn}, index {index}')

    if not har:
        logging.warning('Unable to partition step, null HAR.')
        return

    page = har.get('log').get('pages')[0]
    metadata = page.get('_metadata')
    if metadata.get('crawl_depth') and metadata.get('crawl_depth') != '0':
        # Only home pages have a crawl depth of 0.
        return

    page_url = get_page_url(har)

    if not page_url:
        logging.warning('Skipping HAR: unable to get page URL (see preceding warning).')
        return

    hashed_url = hash_url(page_url)
    if hashed_url % NUM_PARTITIONS != index:
        logging.info(f'Skipping partition. {hashed_url} % {NUM_PARTITIONS} != {index}')
        return
    
    return fn(har)


def hash_url(url):
    """Hashes a given URL to a process-stable integer value."""
    return int(sha256(url.encode('utf-8')).hexdigest(), 16)


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

    return json.dumps(obj, separators=(',', ':'), ensure_ascii=False)


def from_json(val):
    """Returns an object from the JSON representation."""

    try:
        return json.loads(val)
    except Exception as e:
        logging.error('Unable to parse JSON object "%s...": %s' % (val[:50], e))
        return


def run(argv=None):
    """Constructs and runs the BigQuery import pipeline."""
    parser = argparse.ArgumentParser()
    parser.add_argument(
        '--input',
        required=True,
        help='Input Cloud Storage directory to process.')
    known_args, pipeline_args = parser.parse_known_args(argv)
    pipeline_options = PipelineOptions(pipeline_args)
    pipeline_options.view_as(SetupOptions).save_main_session = True

    p = beam.Pipeline(options=pipeline_options)

    hars = (p
        | beam.Create([known_args.input])
        | beam.io.ReadAllFromText()
        | 'MapJSON' >> beam.Map(from_json))

    for i in range(NUM_PARTITIONS):
        _ = (hars
            | f'ParseCss{i}' >> beam.FlatMap(
                (lambda i: lambda har: partition_step(get_parsed_css, har, i))(i))
            | f'WriteParsedCss{i}' >> beam.io.WriteToBigQuery(
                'httparchive:experimental_parsed_css.2022_06_09_test',
                schema='page:STRING, is_home_page:BOOLEAN, url:STRING, css:STRING',
                write_disposition=beam.io.BigQueryDisposition.WRITE_APPEND,
                create_disposition=beam.io.BigQueryDisposition.CREATE_IF_NEEDED))

    pipeline_result = p.run()
    if not isinstance(p.runner, DataflowRunner):
        pipeline_result.wait_until_finish()


if __name__ == '__main__':
    logging.getLogger().setLevel(logging.INFO)
    run()
