import argparse
import gzip
import os
import tempfile
from unittest import TestCase, mock

import apache_beam as beam
from apache_beam.io import WriteToBigQuery
from apache_beam.testing.test_pipeline import TestPipeline
from apache_beam.testing.util import assert_that, equal_to

from modules.transformation import HarJsonToSummary, HarJsonToSummaryDoFn
from modules.transformation import (
    ReadHarFiles,
    add_common_pipeline_options,
    WriteBigQuery,
)


class TestImportHarJson(TestCase):
    def test_generate_pages_none_error(self):
        with self.assertLogs(level="WARNING") as log:
            ret = HarJsonToSummary.generate_pages("foo", None)
            self.assertEqual(len(log.output), 1)
            self.assertEqual(len(log.records), 1)
            self.assertIn("HAR file read error", log.output[0])
            self.assertEqual(ret, (None, None))

    def test_generate_pages_decode_warning(self):
        with self.assertLogs(level="WARNING") as log:
            ret = HarJsonToSummary.generate_pages("foo", "garbage")
            self.assertEqual(len(log.output), 1)
            self.assertEqual(len(log.records), 1)
            self.assertEqual(ret, (None, None))

    def test_generate_pages_empty_error(self):
        with self.assertLogs(level="WARNING") as log:
            ret = HarJsonToSummary.generate_pages("foo", '{"log": {"pages": []}}')
            self.assertEqual(len(log.output), 1)
            self.assertEqual(len(log.records), 1)
            self.assertIn("No pages found", log.output[0])
            self.assertEqual(ret, (None, None))

    def test_import_page_empty_status_info(self):
        with self.assertRaises(Exception):
            HarJsonToSummary.import_page(None, {})

    def test_import_har_json_bad_data(self):
        with self.assertRaises(StopIteration):
            next(HarJsonToSummaryDoFn().process(("file_name", "data")))

    def test_import_har_json(self):
        with mock.patch(
            "modules.transformation.HarJsonToSummary.generate_pages"
        ) as mock_generate_pages:
            expected_page = {"page": "foo"}
            expected_requests = [{"requests": "bar"}]
            mock_generate_pages.return_value = (expected_page, expected_requests)

            with TestPipeline() as p:
                page, requests = (
                    p
                    | beam.Create([("file_name", "data")])
                    | beam.ParDo(HarJsonToSummaryDoFn()).with_outputs(
                        "page", "requests"
                    )
                )
                assert_that(page, equal_to([expected_page]), label="PagesMatch")
                assert_that(
                    requests, equal_to([expected_requests]), label="RequestsMatch"
                )


class TestTransformation(TestCase):
    def test_add_common_pipeline_options(self):
        parser = argparse.ArgumentParser()
        add_common_pipeline_options(parser)

        parsed = parser.parse_args(["--input", "foo"])
        self.assertEqual(parsed.input, "foo")

        parsed = parser.parse_args(["--input_file", "bar"])
        self.assertEqual(parsed.input_file, "bar")


class TestReadHarFiles(TestCase):
    temp_files = []

    def tearDown(self) -> None:
        for f in self.temp_files:
            if os.path.exists(f):
                os.remove(f)

    def write_data(self, data, suffix, compressed=False):
        lines = [data]
        # `delete=False` because windows won't allow concurrent access, must clean up when finished
        with tempfile.NamedTemporaryFile(suffix=suffix, delete=False) as f:
            if compressed:
                with gzip.GzipFile(mode="wb", fileobj=f) as gzf:
                    gzf.write(data)
            else:
                f.write(data)

        self.temp_files.append(f.name)

        return f.name, [line.decode("utf-8") for line in lines]

    def test_input(self):
        with TestPipeline() as p:
            file_name, expected_data = self.write_data(
                b"foo", ".har.gz", compressed=True
            )
            expected_data = [(file_name, line) for line in expected_data]

            pcoll = p | ReadHarFiles(input=file_name)
            assert_that(pcoll, equal_to(expected_data))

    def test_input_file(self):
        with TestPipeline() as p:
            har_file_name, har_expected_data = self.write_data(
                b"foo", ".har.gz", compressed=True
            )
            expected_data = [(har_file_name, line) for line in har_expected_data]

            manifest_file_name, manifest_expected_data = self.write_data(
                har_file_name.encode("utf-8"), ".txt"
            )

            pcoll = p | ReadHarFiles(input_file=manifest_file_name)
            assert_that(pcoll, equal_to(expected_data))


class TestWriteBigQuery(TestCase):
    @staticmethod
    def transformations(table_reference, schema, additional_parameters=None):

        if additional_parameters is None:
            original = WriteBigQuery(table_reference, schema)
        else:
            original = WriteBigQuery(table_reference, schema, additional_parameters)

        p = TestPipeline()
        p | "MyWriteBigQuery" >> original

        # Run the pipeline through to generate a pipeline proto from an empty
        # context. This ensures that the serialization code ran.
        pipeline_proto, context = TestPipeline.from_runner_api(
            p.to_runner_api(), p.runner, p.get_pipeline_options()
        ).to_runner_api(return_context=True)

        # Find the transform from the context.
        write_to_bq_id = [
            k
            for k, v in pipeline_proto.components.transforms.items()
            if v.unique_name == "MyWriteBigQuery"
        ][0]
        deserialized_node = context.transforms.get_by_id(write_to_bq_id)
        return original, deserialized_node.parts[0].transform

    def test_inputs(self):
        additional_parameters = {"baz": "qux"}
        schema = {"fields": [{"name": "foo", "type": "bar"}]}
        project = "project"
        dataset = "dataset"
        table = "table"
        table_reference = f"{project}:{dataset}.{table}"

        _, deserialized = self.transformations(
            table_reference, schema, additional_parameters
        )
        deser_tbl_ref = deserialized.table_reference

        self.assertIsInstance(deserialized, WriteToBigQuery)
        self.assertLessEqual(
            additional_parameters.items(), deserialized.additional_bq_parameters.items()
        )
        self.assertEqual(deserialized.schema, schema)
        self.assertEqual(
            f"{deser_tbl_ref.projectId}:{deser_tbl_ref.datasetId}.{deser_tbl_ref.tableId}",
            table_reference,
        )

    def test_default_parameters(self):
        schema = {"fields": [{"name": "foo", "type": "bar"}]}
        table_reference = "project:dataset.table"

        original, deserialized = self.transformations(table_reference, schema)

        self.assertEqual(deserialized.method, original.default_parameters["method"])
        self.assertEqual(
            deserialized.create_disposition,
            original.default_parameters["create_disposition"],
        )
        self.assertEqual(
            deserialized.write_disposition,
            original.default_parameters["write_disposition"],
        )
        self.assertEqual(
            deserialized.additional_bq_parameters["ignoreUnknownValues"],
            original.default_parameters["additional_bq_parameters"][
                "ignoreUnknownValues"
            ],
        )
