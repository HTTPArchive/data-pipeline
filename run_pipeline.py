import logging

from modules import import_har

if __name__ == "__main__":
    logging.getLogger().setLevel(logging.INFO)
    import_har.run()
