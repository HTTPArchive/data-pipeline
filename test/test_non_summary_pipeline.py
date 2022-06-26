from unittest import TestCase, mock

from modules.non_summary_pipeline import partition_step


class TestNonSummaryPipeline(TestCase):
    @mock.patch("modules.non_summary_pipeline.get_page_url", lambda _: "example.com")
    def test_partition_step(self):
        crawl_depths = [0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10]
        hars = []

        for crawl_depth in crawl_depths:
            hars += {
                "log": {"pages": [{"_metadata": {"crawl_depth": crawl_depth}}]},
                "date": None,
                "client": None,
            }

        partitions = 4
        min_partition = 1

        for har in hars:
            self.assertTrue(min_partition <= partition_step(har, partitions) <= partitions)

    def test_partition_step_empty_har(self):
        har = None
        partitions = 4
        expected_warning = "Unable to partition step, null HAR."
        expected_return = 0

        with self.assertLogs(level="WARNING") as log:
            ret = partition_step(har, partitions)
            self.assertEqual(len(log.output), 1)
            self.assertEqual(len(log.records), 1)
            self.assertIn(expected_warning, log.output[0])
            self.assertEqual(ret, expected_return)

    @mock.patch("modules.non_summary_pipeline.get_page_url", lambda _: None)
    def test_partition_step_empty_page_url(self):
        har = {"foo"}
        partitions = 4
        expected_warning = "Skipping HAR: unable to get page URL"
        expected_return = 0

        with self.assertLogs(level="WARNING") as log:
            ret = partition_step(har, partitions)
            self.assertEqual(len(log.output), 1)
            self.assertEqual(len(log.records), 1)
            self.assertIn(expected_warning, log.output[0])
            self.assertEqual(ret, expected_return)
