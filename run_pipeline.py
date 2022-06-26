#!/usr/bin/env python3

import logging

from apache_beam.runners import DataflowRunner

from modules import combined_pipeline


def run(argv=None):
    logging.getLogger().setLevel(logging.INFO)
    p = combined_pipeline.create_pipeline()
    pipeline_result = p.run(argv)
    if not isinstance(p.runner, DataflowRunner):
        pipeline_result.wait_until_finish()


if __name__ == "__main__":
    run()
