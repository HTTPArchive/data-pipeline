import argparse
import logging

from modules import combined_pipeline, summary_pipeline, non_summary_pipeline, legacy_nonsummary


def parse_args():
    parser = argparse.ArgumentParser()
    pipeline_types = ["combined", "summary", "non_summary"]
    parser.add_argument(
        "--pipeline_type",
        default="combined",
        choices=pipeline_types,
        help=f"Type of pipeline to run. One of {','.join(pipeline_types)}",
    )
    return parser.parse_known_args()


if __name__ == "__main__":
    logging.getLogger().setLevel(logging.INFO)
    known_args, extra_args = parse_args()

    if known_args.pipeline_type == "combined":
        combined_pipeline.run()
    elif known_args.pipeline_type == "summary":
        summary_pipeline.run()
    elif known_args.pipeline_type == "non_summary":
        legacy_nonsummary.run()
