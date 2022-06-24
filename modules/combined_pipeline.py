import argparse
import logging

import apache_beam as beam
from apache_beam.options.pipeline_options import PipelineOptions, StandardOptions
from apache_beam.runners import DataflowRunner

from modules import summary_pipeline, non_summary_pipeline
from modules.summary_pipeline import SummaryPipelineOptions
from modules.transformation import ReadHarFiles, HarJsonToSummaryDoFn


def create_pipeline(argv=None):
    parser = argparse.ArgumentParser()
    known_args, pipeline_args = parser.parse_known_args(argv)
    pipeline_options = PipelineOptions(pipeline_args, save_main_session=True)
    standard_options = pipeline_options.view_as(StandardOptions)
    summary_options = pipeline_options.view_as(SummaryPipelineOptions)
    logging.info(
        f"Pipeline Options: {known_args=},{pipeline_args=},{pipeline_options.get_all_options()},"
        f"{standard_options},{summary_options}"
    )
    if not (summary_options.subscription or summary_options.input):
        raise RuntimeError(
            "Either one of --input or --subscription options must be provided"
        )

    p = beam.Pipeline(options=pipeline_options)

    files = p | ReadHarFiles(summary_options.subscription, summary_options.input)

    # summary pipeline
    # pages, requests = files | "ParseHarToSummary" >> beam.ParDo(
    #     HarJsonToSummaryDoFn()
    # ).with_outputs("page", "requests")
    # pages | summary_pipeline.WriteSummaryPagesToBigQuery(
    #     summary_options, standard_options
    # )
    # requests | summary_pipeline.WriteSummaryRequestsToBigQuery(
    #     summary_options, standard_options
    # )

    # non-summary pipeline
    (
        files
        | "MapJSON" >> beam.MapTuple(non_summary_pipeline.from_json)
        | "AddDateAndClient" >> beam.Map(non_summary_pipeline.add_date_and_client)
        | "WriteNonSummaryTables"
        >> non_summary_pipeline.WriteNonSummaryToBigQuery(pipeline_options)
    )

    return p


def run(argv=None):
    logging.getLogger().setLevel(logging.INFO)
    p = create_pipeline()
    pipeline_result = p.run(argv)
    if not isinstance(p.runner, DataflowRunner):
        pipeline_result.wait_until_finish()


if __name__ == "__main__":
    run()
