from unittest import TestCase

import apache_beam as beam

from modules import import_all


class TestAllPipeline(TestCase):
    def test_create_pipeline_serialization(self):
        # batch/GCS file glob
        p = import_all.create_pipeline(["--input", "foo"])
        beam.Pipeline.from_runner_api(p.to_runner_api(), p.runner, p._options)

        # batch/GCS file listing
        p = import_all.create_pipeline(["--input_file", "bar"])
        beam.Pipeline.from_runner_api(p.to_runner_api(), p.runner, p._options)
