from unittest import TestCase

from transformation import ImportHarJson


class TestImportHarJson(TestCase):
    def test_generate_pages_none_error(self):
        with self.assertRaises(RuntimeError):
            ImportHarJson.generate_pages(None)

    def test_generate_pages_decode_error(self):
        with self.assertRaises(RuntimeError):
            ImportHarJson.generate_pages('garbage')

    def test_generate_pages_empty_error(self):
        with self.assertRaises(RuntimeError):
            ImportHarJson.generate_pages('{"log": {"pages": []}}')

    def test_import_page_empty_status_info(self):
        self.assertIsNone(ImportHarJson.import_page(None, {}))
