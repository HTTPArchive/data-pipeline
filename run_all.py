#!/usr/bin/env python3

import logging

from apache_beam.runners import DataflowRunner

from modules import import_all


def run(argv=None):
    logging.getLogger().setLevel(logging.INFO)
    pipeline = import_all.create_pipeline()
    pipeline_result = pipeline.run(argv)
    if not isinstance(pipeline.runner, DataflowRunner):
        pipeline_result.wait_until_finish()


if __name__ == "__main__":
    run()
