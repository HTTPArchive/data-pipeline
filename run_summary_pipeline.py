import logging

from modules import non_summary_pipeline

if __name__ == "__main__":
    logging.getLogger().setLevel(logging.INFO)
    non_summary_pipeline.run()
