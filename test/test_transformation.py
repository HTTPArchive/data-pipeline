from unittest import TestCase

from modules.transformation import HarJsonToSummary


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
            next(HarJsonToSummary().process(("file_name", "data")))
