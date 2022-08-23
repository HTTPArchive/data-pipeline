from unittest import TestCase

import apache_beam as beam

from modules.combined_pipeline import create_pipeline


class TestCombinedPipeline(TestCase):
    def test_create_pipeline_serialization(self):
        tests = [
            ("input", ["--input", "gs://project/bucket"]),
            ("input_file", ["--input_file", "gs://project/bucket/manifest.txt"]),
            ("pipeline_type_combined", ["--input", "foo", "--pipeline_type", "combined"]),
            ("pipeline_type_summary", ["--input", "foo", "--pipeline_type", "summary"]),
            ("pipeline_type_non_summary", ["--input", "foo", "--pipeline_type", "non-summary"]),
        ]

        for name, input in tests:
            with self.subTest(name, input=input):
                p = create_pipeline(input)
                beam.Pipeline.from_runner_api(p.to_runner_api(), p.runner, p._options)
