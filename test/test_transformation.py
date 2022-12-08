import argparse
import copy
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


class TestHarJsonToSummary(TestCase):
    page_fixture = {
        "_metadata": {
            "rank": 10000000,
            "page_id": 12345,
            "tested_url": "https://www.test.com/",
            "layout": "Desktop",
        },
        "testID": "220101_Dx1",
    }

    har_fixture = {
        "log": {
            "pages": [
                {"foo": "bar", "_metadata": page_fixture["_metadata"]},
            ],
            "entries": [{"baz": "qux"}],
        },
    }

    expected_status_info_fixture = {
        "archive": "All",
        "label": "Jan 1 2022",
        "crawlid": 0,
        "wptid": "220101_Dx1",
        "medianRun": 1,
        "page": "https://www.test.com/",
        "pageid": 12345,
        "rank": 10000000,
        "date": "2022_01_01",
        "client": "desktop",
        "metadata": {
            "layout": "Desktop",
            "page_id": 12345,
            "rank": 10000000,
            "tested_url": "https://www.test.com/",
        },
    }

    file_name_fixture = "chrome-Jan_1_2022/220101_Dx1.har.gz"

    def test_initialize_status_info(self):
        status_info = HarJsonToSummary.initialize_status_info(
            self.file_name_fixture, self.page_fixture
        )
        self.assertDictEqual(self.expected_status_info_fixture, status_info)

    def test_initialize_status_info_pageid(self):
        page = copy.deepcopy(self.page_fixture)
        metadata = page["_metadata"]
        # replace test fixture key named `page_id` with `pageid`
        metadata["pageid"] = metadata.pop("page_id")

        self.assertDictEqual(
            self.expected_status_info_fixture,
            HarJsonToSummary.initialize_status_info(
                self.file_name_fixture, self.page_fixture
            ),
        )

    def test_initialize_status_info_missing_metadata(self):
        tests = [
            ("no_crawlid", "crawlid", "crawlid", 0),
            ("no_tested_url", "tested_url", "page", ""),
            ("no_rank", "rank", "rank", None),
        ]

        for name, metadata_key, status_info_key, expected in tests:
            with self.subTest(
                name,
                metadata_key=metadata_key,
                status_info_key=status_info_key,
                expected=expected,
            ):
                expected_status_info = copy.deepcopy(self.expected_status_info_fixture)
                expected_status_info[status_info_key] = expected
                expected_status_info["metadata"].pop(metadata_key, None)

                page = copy.deepcopy(self.page_fixture)
                page["_metadata"].pop(metadata_key, None)

                self.assertDictEqual(
                    expected_status_info,
                    HarJsonToSummary.initialize_status_info(
                        self.file_name_fixture, page
                    ),
                )

    def test_initialize_status_info_default_testid(self):
        page = copy.deepcopy(self.page_fixture)
        page.pop("testID", None)
        self.assertDictEqual(
            self.expected_status_info_fixture,
            HarJsonToSummary.initialize_status_info(self.file_name_fixture, page),
        )

    def test_initialize_status_info_default_layout(self):
        page = copy.deepcopy(self.page_fixture)
        page["_metadata"].pop("layout", None)
        expected_status_info = copy.deepcopy(self.expected_status_info_fixture)
        expected_status_info["metadata"].pop("layout", None)
        self.assertDictEqual(
            expected_status_info,
            HarJsonToSummary.initialize_status_info(self.file_name_fixture, page),
        )

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

    def test_generate_pages_unexpected_element_type(self):
        with self.assertLogs(level="WARNING") as log:
            ret = HarJsonToSummary.generate_pages("foo", 1)
            self.assertEqual(1, len(log.output))
            self.assertEqual(1, len(log.records))
            self.assertIn("Unexpected type for", log.output[0])
            self.assertEqual((None, None), ret)

    def test_generate_pages_import_page_empty_error(self):
        with self.assertLogs(level="WARNING") as log, mock.patch(
            "modules.transformation.HarJsonToSummary.initialize_status_info",
            return_value=self.expected_status_info_fixture,
        ):
            ret = HarJsonToSummary.generate_pages("foo", '{"log": {"pages": [{}]}}')
            self.assertEqual(1, len(log.output))
            self.assertEqual(1, len(log.records))
            self.assertIn("import_page() failed for status_info", log.output[0])
            self.assertEqual((None, None), ret)

    def test_generate_pages_import_entries_empty_error(self):
        with self.assertLogs(level="WARNING") as log, mock.patch(
            "modules.transformation.HarJsonToSummary.initialize_status_info",
            return_value=self.expected_status_info_fixture,
        ), mock.patch(
            "modules.transformation.HarJsonToSummary.import_page", return_value=dict()
        ), mock.patch(
            "modules.transformation.HarJsonToSummary.import_entries",
            return_value=([], "", ""),
        ):
            ret = HarJsonToSummary.generate_pages(
                "foo", '{"log": {"pages": [{}], "entries": []}}'
            )
            self.assertEqual(1, len(log.output))
            self.assertEqual(1, len(log.records))
            self.assertIn("import_entries() failed for status_info", log.output[0])
            self.assertEqual((None, None), ret)

    def test_generate_pages_aggregate_stats_empty_error(self):
        with self.assertLogs(level="WARNING") as log, mock.patch(
            "modules.transformation.HarJsonToSummary.initialize_status_info",
            return_value=self.expected_status_info_fixture,
        ), mock.patch(
            "modules.transformation.HarJsonToSummary.import_page", return_value=dict()
        ), mock.patch(
            "modules.transformation.HarJsonToSummary.import_entries",
            return_value=([dict()], "", ""),
        ), mock.patch(
            "modules.transformation.HarJsonToSummary.aggregate_stats",
            return_value=dict(),
        ):
            ret = HarJsonToSummary.generate_pages(
                "foo", '{"log": {"pages": [{}], "entries": []}}'
            )
            self.assertEqual(1, len(log.output))
            self.assertEqual(1, len(log.records))
            self.assertIn("aggregate_stats() failed for status_info", log.output[0])
            self.assertEqual((None, None), ret)

    def test_generate_pages_dict(self):
        self.assertTupleEqual(
            (None, None),
            HarJsonToSummary.generate_pages(self.file_name_fixture, self.har_fixture),
        )

    def test_import_page_empty_status_info(self):
        with self.assertRaises(Exception):
            HarJsonToSummary.import_page(None, {})

    def test_generate_pages(self):
        with mock.patch(
            "modules.transformation.HarJsonToSummary.initialize_status_info",
            return_value=self.expected_status_info_fixture,
        ), mock.patch(
            "modules.transformation.HarJsonToSummary.import_page", return_value=dict()
        ), mock.patch(
            "modules.transformation.HarJsonToSummary.import_entries",
            return_value=([dict()], "", ""),
        ), mock.patch(
            "modules.transformation.HarJsonToSummary.aggregate_stats",
            return_value={"foo": "bar"},
        ):
            page, entries = HarJsonToSummary.generate_pages(
                self.file_name_fixture, self.har_fixture
            )
            self.assertDictEqual({"foo": "bar"}, page)
            self.assertEqual([dict()], entries)


class TestHarJsonToSummaryDoFn(TestCase):
    def test_import_har_json_bad_data(self):
        with self.assertRaises(StopIteration):
            next(HarJsonToSummaryDoFn().process(("file_name", "data")))

    @mock.patch("modules.transformation.HarJsonToSummary.generate_pages", RuntimeError)
    def test_import_har_json_exception(self):
        with self.assertLogs(level="ERROR") as log:
            with self.assertRaises(Exception):
                next(HarJsonToSummaryDoFn().process(("file_name", "data")))
            self.assertIn("Unable to unpack HAR", log.output[0])

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
        _ = p | "MyWriteBigQuery" >> original

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
