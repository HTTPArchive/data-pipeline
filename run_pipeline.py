import logging

from modules import summary_pipeline

if __name__ == "__main__":
    logging.getLogger().setLevel(logging.INFO)
    summary_pipeline.run()
