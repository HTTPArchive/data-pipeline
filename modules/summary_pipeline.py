import apache_beam as beam

from modules import constants, utils, transformation


class WriteSummaryPagesToBigQuery(beam.PTransform):
    def __init__(self, summary_options, standard_options, label=None, **kwargs):
        super().__init__(label)
        self.summary_options = summary_options
        self.standard_options = standard_options

    def expand(self, pages):
        home_pages = pages | "FilterHomePages" >> beam.Filter(utils.is_home_page)

        _ = (  # pragma: no branch
            home_pages
            | "WriteHomePagesToBigQuery"
            >> transformation.WriteBigQuery(
                table=lambda row: utils.format_table_name(
                    row, self.summary_options.dataset_summary_pages_home_only
                ),
                schema=constants.BIGQUERY["schemas"]["summary_pages"],
            )
        )


class WriteSummaryRequestsToBigQuery(beam.PTransform):
    def __init__(self, summary_options, standard_options, label=None, **kwargs):
        super().__init__(label)
        self.summary_options = summary_options
        self.standard_options = standard_options

    def expand(self, requests):
        requests = requests | "FlattenRequests" >> beam.FlatMap(  # pragma: no branch
            lambda elements: elements
        )

        home_requests = requests | "FilterHomeRequests" >> beam.Filter(
            utils.is_home_page
        )

        _ = (  # pragma: no branch
            home_requests
            | "WriteHomeRequestsToBigQuery"
            >> transformation.WriteBigQuery(
                table=lambda row: utils.format_table_name(
                    row, self.summary_options.dataset_summary_requests_home_only
                ),
                schema=constants.BIGQUERY["schemas"]["summary_requests"],
            )
        )
