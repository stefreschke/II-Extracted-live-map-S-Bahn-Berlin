"""
Setup logging for project.
Logfiles are supposed to be written to ./logs/!
Console output is supposed to be delivered.
"""
import logging
import os


def init_logger():
    """
    Setup logger, calls debug_logger. Formatter generation happens here.
    :return:
    """
    formatter = logging.Formatter(fmt='%(asctime)s %(levelname)-8s %(message)s',
                                  datefmt='%Y-%m-%d %H:%M:%S')
    debug_logger(formatter)


def debug_logger(formatter):
    """
    Setup logger based on already generated formatter.
    :param formatter: Given formatter.
    :return:
    """
    console_handler = logging.StreamHandler()
    console_handler.setFormatter(formatter)
    os.makedirs("../logs/", exist_ok=True)
    file_handler = logging.FileHandler(filename="../logs/log.log")
    file_handler.setFormatter(formatter)
    logger = logging.getLogger('extraction')
    logger.setLevel(logging.INFO)
    # logger.addHandler(file_handler)
    logger.addHandler(console_handler)
