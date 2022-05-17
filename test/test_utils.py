import datetime
from unittest import TestCase

from modules import utils


class Test(TestCase):
    def test_get_url_hash(self):
        self.assertEqual(utils.get_url_hash("https://google.com/"), 63524)

    def test_get_url_hash_empty_string(self):
        self.assertEqual(utils.get_url_hash(""), 54301)

    def test_get_ext_question(self):
        self.assertEqual(utils.get_ext("http://test.com/foo.bar?baz"), "bar")

    def test_get_ext_multiple_dot(self):
        self.assertEqual(utils.get_ext("http://test.com/foo.bar.baz"), "baz")

    def test_get_ext_long(self):
        self.assertEqual(utils.get_ext("http://test.com/foo.barbaz"), "")

    def test_pretty_type_mimetype(self):
        tests = [
            ("font", "font"),
            ("css", "css"),
            ("image", "image"),
            ("script", "script"),
            ("video", "video"),
            ("audio", "audio"),
            ("xml", "xml"),
            ("json", "script"),
            ("flash", "video"),
            ("webm", "video"),
            ("mp4", "video"),
            ("flv", "video"),
            ("html", "html"),
            ("text", "text"),
        ]

        for given, expect in tests:
            with self.subTest(given=given, expect=expect):
                self.assertEqual(utils.pretty_type(given, ""), expect)

    def test_pretty_type_ext(self):
        tests = [
            ("js", "script"),
            ("json", "script"),
            ("eot", "font"),
            ("ttf", "font"),
            ("woff", "font"),
            ("woff2", "font"),
            ("otf", "font"),
            ("png", "image"),
            ("gif", "image"),
            ("jpg", "image"),
            ("jpeg", "image"),
            ("webp", "image"),
            ("ico", "image"),
            ("svg", "image"),
            ("avif", "image"),
            ("jxl", "image"),
            ("heic", "image"),
            ("heif", "image"),
            ("css", "css"),
            ("xml", "xml"),
            ("mp4", "video"),
            ("webm", "video"),
            ("ts", "video"),
            ("m4v", "video"),
            ("m4s", "video"),
            ("mov", "video"),
            ("ogv", "video"),
            ("swf", "video"),
            ("f4v", "video"),
            ("flv", "video"),
            ("html", "html"),
            ("htm", "html"),
        ]

        for given, expect in tests:
            with self.subTest(given=given, expect=expect):
                self.assertEqual(utils.pretty_type("", given), expect)

    def test_pretty_type_other(self):
        self.assertEqual(utils.pretty_type("foo", "bar"), "other")

    def test_get_format(self):
        # pretty_type, mime_type/ext, expectation
        tests = [
            ("image", "jpg", "jpg"),
            ("image", "png", "png"),
            ("image", "gif", "gif"),
            ("image", "webp", "webp"),
            ("image", "svg", "svg"),
            ("image", "ico", "ico"),
            ("image", "avif", "avif"),
            ("image", "jxl", "jxl"),
            ("image", "heic", "heic"),
            ("image", "heif", "heif"),
            ("image", "jpeg", "jpg"),
            ("video", "flash", "flash"),
            ("video", "swf", "swf"),
            ("video", "mp4", "mp4"),
            ("video", "flv", "flv"),
            ("video", "f4v", "f4v"),
        ]

        for given_pretty_type, given_type, expect in tests:
            with self.subTest(
                given_pretty_type=given_pretty_type,
                given_type=given_type,
                expect=expect,
            ):
                self.assertEqual(
                    utils.get_format(given_pretty_type, given_type, given_type), expect
                )

    def test_get_format_exceptional(self):
        self.assertEqual(utils.get_format("foo", "bar", "baz"), "")

    def test_datetime_to_epoch_empty(self):
        self.assertIsNone(utils.datetime_to_epoch("not-a-date", {"foo": "bar"}))

    def test_crawl_date(self):
        dir_name = "gs://httparchive/crawls/android-Apr_1_2022"
        self.assertEqual(utils.crawl_date(dir_name), datetime.datetime(2022, 4, 1, 0, 0))

    def test_clamp_integer_normal(self):
        self.assertEqual(utils.clamp_integer(1000), 1000)

    def test_clamp_integer_negative(self):
        self.assertEqual(utils.clamp_integer(-1000), -1000)

    def test_clamp_integer_str(self):
        self.assertEqual(utils.clamp_integer('1000'), 1000)

    def test_clamp_integer_bigint(self):
        self.assertEqual(utils.clamp_integer(2**64), utils.BIGQUERY_MAX_INT)
