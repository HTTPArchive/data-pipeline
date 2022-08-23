import datetime
from unittest import TestCase, mock

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

    def test_get_ext_no_dot(self):
        self.assertEqual(utils.get_ext("http://test.com/foo"), "")

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
        with self.assertLogs(level="WARNING") as log:
            self.assertIsNone(utils.datetime_to_epoch("not-a-date", {"foo": "bar"}))
            self.assertIn("Could not parse datetime to epoch", log.output[0])

    def test_crawl_date(self):
        dir_name = "gs://httparchive/crawls/android-Apr_1_2022"
        self.assertEqual(
            utils.crawl_date(dir_name), datetime.datetime(2022, 4, 1, 0, 0)
        )

    def test_clamp_integer_normal(self):
        self.assertEqual(utils.clamp_integer(1000), 1000)

    def test_clamp_integer_negative(self):
        self.assertEqual(utils.clamp_integer(-1000), None)

    def test_clamp_integer_str(self):
        self.assertEqual(utils.clamp_integer("1000"), 1000)

    def test_clamp_integer_bigint(self):
        self.assertEqual(utils.clamp_integer(2**64), utils.BIGQUERY_MAX_INT)

    def test_clamp_integers(self):
        b = utils.BIGQUERY_MAX_INT + 10
        cols = ["a", "b", "c", "d"]
        data = {"a": 1, "b": b, "c": None}
        with self.assertLogs(level="WARNING") as log:
            utils.clamp_integers(data, cols)
            self.assertEqual(data["a"], 1)
            self.assertEqual(data["b"], utils.BIGQUERY_MAX_INT)
            self.assertEqual(data["c"], None)
            self.assertIn("Clamping required for {'b': " + str(b), log.output[0])

    def test_format_table_name(self):
        project_dataset = "project_name:dataset_name"
        row = {"date": "2022-01-01", "client": "test"}
        self.assertEqual(
            utils.format_table_name(row, project_dataset),
            f"{project_dataset}.{row['date']}_{row['client']}",
        )

    def test_format_table_name_exception(self):
        with self.assertLogs(level="ERROR") as log:
            self.assertRaises(Exception, utils.format_table_name, None, None)
            self.assertRaises(
                Exception, utils.format_table_name, dict(), "project_name:dataset_name"
            )
            self.assertEqual(len(log), 2)
            self.assertIn("Unable to determine full table name.", log.output[0])

    def test_dict_subset(self):
        tests = [
            ("contains_all_keys", {"foo": "bar"}, ["foo"], {"foo": "bar"}),
            (
                "contains_some_keys",
                {"foo": "bar", "baz": "qux"},
                ["foo"],
                {"foo": "bar"},
            ),
            ("contains_no_keys", {"baz": "qux"}, ["foo"], dict()),
        ]

        for name, input_dict, input_keys, expected_dict in tests:
            with self.subTest(
                name,
                input_dict=input_dict,
                input_keys=input_keys,
                expected_dict=expected_dict,
            ):
                self.assertDictEqual(
                    expected_dict, utils.dict_subset(input_dict, input_keys)
                )

    def test_dict_subset_empty_dict(self):
        self.assertIsNone(utils.dict_subset(None, None))
        self.assertIsNone(utils.dict_subset(dict(), None))

    def test_dict_subset_no_keys(self):
        self.assertIsNone(utils.dict_subset({"foo": "bar"}, None))
        self.assertIsNone(utils.dict_subset({"foo": "bar"}, []))

    def test_date_and_client_from_file_name(self):
        self.assertTupleEqual(
            (datetime.datetime.fromisoformat("2022-01-01"), "desktop"),
            utils.date_and_client_from_file_name("/chrome-Jan_1_2022/foo.har.gz"),
        )

    def test_client_name(self):
        tests = [
            ("/chrome-Jan_1_2022/foo.har.gz", "desktop"),
            ("/android-Jan_1_2022/foo.har.gz", "mobile"),
            ("/foo/bar_Dx123.har.gz", "desktop"),
            ("/foo/bar_Mx123.har.gz", "mobile"),
            ("/foo/bar.har.gz", "foo"),
        ]

        for given, expect in tests:
            with self.subTest(given=given, expect=expect):
                self.assertEqual(utils.client_name(given), expect)

    @mock.patch.dict(
        "modules.constants.BIGQUERY",
        {
            "schemas": {
                "test_table": {
                    "fields": [
                        {"name": "foo", "type": "STRING"},
                        {"name": "bar", "type": "INTEGER"},
                    ]
                }
            }
        },
    )
    def test_int_columns_for_schema(self):
        self.assertEqual(["bar"], utils.int_columns_for_schema("test_table"))

    def test_is_home_page(self):
        tests = [
            ("no_metadata", dict(), True),
            ("depth_1", {"metadata": '{"crawl_depth": 0}'}, True),
            ("depth_2", {"metadata": '{"crawl_depth": 1}'}, False),
        ]

        for name, given, expected in tests:
            with self.subTest(name, given=given, expected=expected):
                self.assertEqual(expected, utils.is_home_page(given))

    def test_parse_header(self):
        tests = [
            (
                "standard_header",
                ({"req_foo": ["bar"]}, "", 0),
                [{"name": "foo", "value": "bar"}],
                {"foo": "req_foo"},
                None,
                None,
            ),
            (
                "cookie_key",
                (dict(), "", len("bar")),
                [{"name": "foo", "value": "bar"}],
                dict(),
                "foo",
                None,
            ),
            (
                "other_header",
                (dict(), "foo = bar", 0),
                [{"name": "foo", "value": "bar"}],
                dict(),
                None,
                None,
            ),
            (
                "combine_input_output",
                ({"req_foo": ["bar"], "baz": ["qux"]}, "", 0),
                [{"name": "foo", "value": "bar"}],
                {"foo": "req_foo"},
                None,
                {"baz": ["qux"]},
            ),
            (
                "append_output",
                ({"req_foo": ["bar", "baz"]}, "", 0),
                [{"name": "foo", "value": "bar"}, {"name": "foo", "value": "baz"}],
                {"foo": "req_foo"},
                None,
                None,
            ),
        ]

        for (
            name,
            expected,
            input_headers,
            standard_headers,
            cookie_key,
            output_headers,
        ) in tests:
            with self.subTest(
                name,
                input_headers=input_headers,
                standard_headers=standard_headers,
                cookie_key=cookie_key,
                output_headers=output_headers,
            ):
                self.assertTupleEqual(
                    expected,
                    utils.parse_header(
                        input_headers, standard_headers, cookie_key, output_headers
                    ),
                )
