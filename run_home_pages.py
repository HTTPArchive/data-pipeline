#!/usr/bin/env python3

import logging

from apache_beam.runners import DataflowRunner

from modules import import_home_pages


def run(argv=None):
    logging.getLogger().setLevel(logging.INFO)
    pipeline = import_home_pages.create_pipeline()
    pipeline_result = pipeline.run(argv)
    if not isinstance(pipeline.runner, DataflowRunner):
        pipeline_result.wait_until_finish()


if __name__ == "__main__":
    run()
