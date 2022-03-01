from unittest import TestCase

from transformation import ImportHarJson


class TestImportHarJson(TestCase):
    def test_get_url_hash(self):
        self.assertEqual(ImportHarJson.get_url_hash("https://google.com/"), 63524)

    def test_generate_pages_none_error(self):
        with self.assertRaises(RuntimeError):
            ImportHarJson.generate_pages(None)

    def test_generate_pages_decode_error(self):
        with self.assertRaises(RuntimeError):
            ImportHarJson.generate_pages('garbage')

    def test_generate_pages_empty_error(self):
        with self.assertRaises(RuntimeError):
            ImportHarJson.generate_pages('{"log": {"pages": []}}')

