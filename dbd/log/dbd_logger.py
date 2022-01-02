import logging

FORMAT = "%(asctime)s %(levelname)s %(module)s '%(message)s'"


def setup_logging():
    logging.basicConfig(level=logging.DEBUG, format=FORMAT, filename='dbd.log')
