from unittest import TestCase

from modules.transformation import ImportHarJson


class TestImportHarJson(TestCase):
    def test_generate_pages_none_error(self):
        with self.assertRaises(RuntimeError):
            ImportHarJson.generate_pages("foo", None)

    def test_generate_pages_decode_error(self):
        with self.assertRaises(RuntimeError):
            ImportHarJson.generate_pages("foo", "garbage")

    def test_generate_pages_empty_error(self):
        with self.assertRaises(RuntimeError):
            ImportHarJson.generate_pages("foo", '{"log": {"pages": []}}')

    def test_import_page_empty_status_info(self):
        self.assertIsNone(ImportHarJson.import_page(None, {}))
