import argparse
import logging

import apache_beam as beam
from apache_beam.options.pipeline_options import PipelineOptions, StandardOptions
from apache_beam.runners import DataflowRunner

from modules import non_summary_pipeline, summary_pipeline
from modules.non_summary_pipeline import NonSummaryPipelineOptions
from modules.summary_pipeline import SummaryPipelineOptions
from modules.transformation import ReadHarFiles, HarJsonToSummaryDoFn


def create_pipeline(argv=None):
    parser = argparse.ArgumentParser()
    known_args, pipeline_args = parser.parse_known_args(argv)
    pipeline_options = PipelineOptions(pipeline_args, save_main_session=True)
    standard_options = pipeline_options.view_as(StandardOptions)
    summary_options = pipeline_options.view_as(SummaryPipelineOptions)
    non_summary_options = pipeline_options.view_as(NonSummaryPipelineOptions)
    logging.info(f"Pipeline Options: {known_args=},{pipeline_args=},{pipeline_options},{standard_options},"
                 f"{summary_options},{non_summary_options}")
    if not (summary_options.subscription or summary_options.input):
        raise RuntimeError(
            "Either one of --input or --subscription options must be provided"
        )

    p = beam.Pipeline(options=pipeline_options)

    files = p | ReadHarFiles(summary_options.subscription, summary_options.input)

    (files
     | "ParseHarToSummary" >> beam.ParDo(HarJsonToSummaryDoFn()).with_outputs("page", "requests")
     | "WriteSummaryTables" >> summary_pipeline.WriteBigQuery(known_args, standard_options))

    (files
     | "MapJSON" >> beam.MapTuple(non_summary_pipeline.from_json)
     | "AddDateAndClient" >> beam.Map(non_summary_pipeline.add_date_and_client)
     | "WriteNonSummaryTables" >> non_summary_pipeline.WriteBigQuery(non_summary_options))

    return p


def run(argv=None):
    logging.getLogger().setLevel(logging.INFO)
    p = create_pipeline()
    pipeline_result = p.run(argv)
    if not isinstance(p.runner, DataflowRunner):
        pipeline_result.wait_until_finish()


if __name__ == '__main__':
    run()
