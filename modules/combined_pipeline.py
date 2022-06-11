import argparse
import logging

import apache_beam as beam
from apache_beam.options.pipeline_options import PipelineOptions, StandardOptions
from apache_beam.runners import DataflowRunner

from modules.non_summary_pipeline import WriteBigQuery, NonSummaryPipelineOptions
from modules.summary_pipeline import _WriteToBigQuery, SummaryPipelineOptions
from modules.transformation import ReadHarFiles


class WriteSummaryTables(beam.PTransform):
    def __init__(self, known_args, standard_options, label=None):
        super().__init__(label)
        self.known_args = known_args
        self.standard_options = standard_options

    def expand(self, pcoll):
        return _WriteToBigQuery(self.known_args, self.standard_options)


class WriteNonSummaryTables(beam.PTransform):
    def __init__(self, known_args, label=None):
        super().__init__(label)
        self.known_args = known_args

    def expand(self, pcoll):
        return pcoll | WriteBigQuery(self.known_args)


def create_pipeline(argv=None):
    parser = argparse.ArgumentParser()
    known_args, pipeline_args = parser.parse_known_args(argv)
    logging.info(f"Pipeline Options: known_args={known_args},pipeline_args={pipeline_args}")
    pipeline_options = PipelineOptions(pipeline_args, save_main_session=True)
    standard_options = pipeline_options.view_as(StandardOptions)
    summary_options = pipeline_options.view_as(SummaryPipelineOptions)
    non_summary_options = pipeline_options.view_as(NonSummaryPipelineOptions)
    if not (summary_options.subscription or summary_options.input):
        raise RuntimeError(
            "Either one of --input or --subscription options must be provided"
        )

    p = beam.Pipeline(options=pipeline_options)

    files = p | ReadHarFiles(summary_options.subscription, summary_options.input)
    files | WriteSummaryTables(summary_options, standard_options)
    files | WriteNonSummaryTables(non_summary_options)

    return p


def run(argv=None):
    logging.getLogger().setLevel(logging.INFO)
    p = create_pipeline()
    pipeline_result = p.run(argv)
    if not isinstance(p.runner, DataflowRunner):
        pipeline_result.wait_until_finish()


if __name__ == '__main__':
    run()
