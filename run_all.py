import logging

from modules import import_all

if __name__ == "__main__":
    logging.getLogger().setLevel(logging.INFO)
    import_all.run()
