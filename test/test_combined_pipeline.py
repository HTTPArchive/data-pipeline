from unittest import TestCase

import apache_beam as beam

from modules.combined_pipeline import create_pipeline


class TestCombinedPipeline(TestCase):
    def test_create_pipeline_serialization(self):
        # batch/GCS file glob
        p = create_pipeline(["--input", "foo"])
        beam.Pipeline.from_runner_api(p.to_runner_api(), p.runner, p._options)

        # batch/GCS file listing
        p = create_pipeline(["--input_file", "bar"])
        beam.Pipeline.from_runner_api(p.to_runner_api(), p.runner, p._options)

        # streaming/pubsub
        p = create_pipeline(
            ["--subscription", "projects/httparchive/subscriptions/foo"]
        )
        beam.Pipeline.from_runner_api(p.to_runner_api(), p.runner, p._options)
