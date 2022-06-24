import argparse
import json
import logging

import apache_beam as beam
from apache_beam.options.pipeline_options import PipelineOptions
from apache_beam.runners import DataflowRunner

from modules import constants, utils
from modules.css_parser import parse as parse_css


# BigQuery can handle rows up to 100 MB.
MAX_CONTENT_SIZE = 10 * 1024 * 1024


def get_parsed_css(har):
    """Returns a list of stylesheets contained in the HAR."""

    if not har:
        return None
    
    page = har.get('log').get('pages')[0]
    page_url = page.get('_URL')
    metadata = page.get('_metadata')
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

        response_body = response.get('text')
        ast = parse_css(response_body)
        css = json.dumps(ast)

        if len(css) > MAX_CONTENT_SIZE:
            continue

        parsed_css.append({
            'date': har.get('date'),
            'client': har.get('client'),
            'page': page,
            'url': request_url,
            'css': css
        })

    return parsed_css

def from_json(file_name, element):
    """Returns an object from the JSON representation."""
    try:
        return file_name, json.loads(element)
    except json.JSONDecodeError as err:
        logging.error('Unable to parse JSON object "%s...": %s', element[:50], err)
        return None, None


def add_date_and_client(element):
    """Adds date and client attributes to the HAR."""
    file_name, har = element
    date, client = utils.date_and_client_from_file_name(file_name)
    page = har.get('log').get('pages')[0]
    metadata = page.get('_metadata', {})
    har.update(
        {
            'date': '{:%Y_%m_%d}'.format(date),
            'client': metadata.get('layout', client).lower(),
        }
    )

    return har
        

def run(argv=None):
    """Constructs and runs the CSS parsing pipeline."""
    parser = argparse.ArgumentParser()
    parser.add_argument(
        '--input',
        required=True,
        help='GCS path pointing to the HAR directory'
    )
    known_args, pipeline_args = parser.parse_known_args(argv)
    pipeline_options = PipelineOptions(pipeline_args, save_main_session=True)

    pipeline = beam.Pipeline(options=pipeline_options)

    _ = (
        pipeline
        | beam.Create([known_args.input])
        | beam.io.ReadAllFromText(with_filename=True)
        | 'MapJSON' >> beam.MapTuple(from_json)
        | 'AddDateAndClient' >> beam.Map(add_date_and_client)
        | 'GetParsedCSS' >> beam.FlatMap(get_parsed_css)
        | 'WriteParsedCSS' >> beam.io.WriteToBigQuery(
            'httparchive:scratchspace.parsed_css_test',
            schema=constants.bigquery['schemas']['parsed_css'],
            write_disposition=beam.io.BigQueryDisposition.WRITE_APPEND,
            create_disposition=beam.io.BigQueryDisposition.CREATE_NEVER
        )
    )

    pipeline_result = pipeline.run()
    if not isinstance(pipeline.runner, DataflowRunner):
        pipeline_result.wait_until_finish()
