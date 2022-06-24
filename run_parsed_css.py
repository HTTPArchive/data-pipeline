import logging

from modules import parse_stylesheets

if __name__ == "__main__":
    logging.getLogger().setLevel(logging.INFO)
    parse_stylesheets.run()
