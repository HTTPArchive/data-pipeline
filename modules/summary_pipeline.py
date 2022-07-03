import apache_beam as beam
from apache_beam.io.gcp import bigquery

from modules import constants, utils, transformation
from modules.transformation import add_deadletter_logging


class WriteSummaryPagesToBigQuery(beam.PTransform):
    def __init__(self, summary_options, standard_options, label=None, triggering_frequency=None, **kwargs):
        super().__init__(label)
        self.summary_options = summary_options
        self.standard_options = standard_options
        self.triggering_frequency = triggering_frequency

    def expand(self, pages):
        home_pages = pages | "FilterHomePages" >> beam.Filter(utils.is_home_page)

        deadletter_queues = {
            # "pages": pages
            # | "WritePagesToBigQuery"
            # >> transformation.WriteBigQuery(
            #     table=lambda row: utils.format_table_name(
            #         row, self.summary_options.dataset_summary_pages
            #     ),
            #     schema=constants.BIGQUERY["schemas"]["summary_pages"],
            #     streaming=self.standard_options.streaming,
            #     method=self.summary_options.big_query_write_method,
            #     triggering_frequency=self.triggering_frequency,
            # ),
            "home_pages": home_pages
            | "WriteHomePagesToBigQuery"
            >> transformation.WriteBigQuery(
                table=lambda row: utils.format_table_name(
                    row, self.summary_options.dataset_summary_pages_home_only
                ),
                schema=constants.BIGQUERY["schemas"]["summary_pages"],
                streaming=self.standard_options.streaming,
                method=self.summary_options.big_query_write_method,
                triggering_frequency=self.triggering_frequency
            ),
        }

        if self.summary_options.big_query_write_method == bigquery.WriteToBigQuery.Method.STREAMING_INSERTS:
            add_deadletter_logging(deadletter_queues)


class WriteSummaryRequestsToBigQuery(beam.PTransform):
    def __init__(self, summary_options, standard_options, triggering_frequency=None, label=None, **kwargs):
        super().__init__(label)
        self.summary_options = summary_options
        self.standard_options = standard_options
        self.triggering_frequency = triggering_frequency

    def expand(self, requests):
        requests = requests | "FlattenRequests" >> beam.FlatMap(
            lambda elements: elements
        )

        home_requests = requests | "FilterHomeRequests" >> beam.Filter(
            utils.is_home_page
        )

        deadletter_queues = {
            # "requests": requests
            # | "WriteRequestsToBigQuery"
            # >> transformation.WriteBigQuery(
            #     table=lambda row: utils.format_table_name(
            #         row, self.summary_options.dataset_summary_requests
            #     ),
            #     schema=constants.BIGQUERY["schemas"]["summary_requests"],
            #     streaming=self.standard_options.streaming,
            #     method=self.summary_options.big_query_write_method,
            #     triggering_frequency = self.triggering_frequency,
            # ),
            "home_requests": home_requests
            | "WriteHomeRequestsToBigQuery"
            >> transformation.WriteBigQuery(
                table=lambda row: utils.format_table_name(
                    row, self.summary_options.dataset_summary_requests_home_only
                ),
                schema=constants.BIGQUERY["schemas"]["summary_requests"],
                streaming=self.standard_options.streaming,
                method=self.summary_options.big_query_write_method,
                triggering_frequency=self.triggering_frequency,
            ),
        }

        if self.summary_options.big_query_write_method == bigquery.WriteToBigQuery.Method.STREAMING_INSERTS:
            add_deadletter_logging(deadletter_queues)
