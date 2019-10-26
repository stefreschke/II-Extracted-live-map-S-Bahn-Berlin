import logging
import os

def init_logger():
    formatter = logging.Formatter(fmt='%(asctime)s %(levelname)-8s %(message)s',
                                  datefmt='%Y-%m-%d %H:%M:%S')
    debug_logger(formatter)


def debug_logger(formatter):
    console_handler = logging.StreamHandler()
    console_handler.setFormatter(formatter)
    os.makedirs("../logs/", exist_ok=True)
    file_handler = logging.FileHandler(filename="../logs/log.log")
    file_handler.setFormatter(formatter)
    logger = logging.getLogger('extraction')
    logger.setLevel(logging.INFO)
    #logger.addHandler(file_handler)
    logger.addHandler(console_handler)
