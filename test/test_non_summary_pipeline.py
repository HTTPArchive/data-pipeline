from unittest import TestCase

from modules.non_summary_pipeline import partition_step


class TestNonSummaryPipeline(TestCase):
    def test_partition_step_secondary_page(self):
        # test for failure case / zero
        har = {"log": {"pages": [{"_metadata": {"crawl_depth": 1}}]}}
        partitions = 4
        self.assertEqual(partition_step(har, partitions), 0)
