import logging

from modules import combined_pipeline

if __name__ == "__main__":
    logging.getLogger().setLevel(logging.INFO)
    combined_pipeline.run()
