from unittest import TestCase

import apache_beam as beam

from modules import import_all


class TestAllPipeline(TestCase):
    def test_create_pipeline_serialization(self):
        tests = [
            ("input", ["--input", "gs://project/bucket"]),
            ("input_file", ["--input_file", "gs://project/bucket/manifest.txt"]),
        ]

        for name, input in tests:
            with self.subTest(name, input=input):
                p = import_all.create_pipeline(input)
                beam.Pipeline.from_runner_api(p.to_runner_api(), p.runner, p._options)
