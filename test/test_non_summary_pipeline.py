from unittest import TestCase, mock

from modules.non_summary_pipeline import partition_step


class TestNonSummaryPipeline(TestCase):

    @mock.patch("modules.non_summary_pipeline.get_page_url", lambda _: "example.com")
    def test_partition_step_home_page(self):
        har = {"log": {"pages": [{"_metadata": {"crawl_depth": 0}}]}, "date": None, "client": None}
        partitions = 4
        min_partition = 1
        self.assertTrue(min_partition <= partition_step(har, partitions) <= partitions)

    def test_partition_step_secondary_page(self):
        # test for failure case / zero
        har = {"log": {"pages": [{"_metadata": {"crawl_depth": 1}}]}}
        partitions = 4
        self.assertEqual(partition_step(har, partitions), 0)
