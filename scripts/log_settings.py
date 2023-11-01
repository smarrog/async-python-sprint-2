import logging
import sys
import os

LOG_FILE = 'logs.log'
ERRORS_LOG_FILE = 'errors.log'
LOGS_DIRECTORY = 'logs'
LOG_FORMAT = '%(asctime)s  =>  %(threadName)-15.15s  =>  %(levelname)-10.10s  =>  %(message)s'
MIN_LOG_LEVEL = logging.DEBUG
CONSOLE_LOG_LEVEL = logging.DEBUG
FILE_LOG_LEVEL = logging.INFO


def init() -> None:
    os.makedirs(LOGS_DIRECTORY, exist_ok=True)

    file_handler = logging.FileHandler(filename=os.path.join(LOGS_DIRECTORY, LOG_FILE), mode='w')
    file_handler.setLevel(FILE_LOG_LEVEL)

    file_errors_handler = logging.FileHandler(filename=os.path.join(LOGS_DIRECTORY, ERRORS_LOG_FILE), mode='w')
    file_errors_handler.setLevel(logging.ERROR)

    console_handler = logging.StreamHandler(sys.stdout)
    console_handler.setLevel(CONSOLE_LOG_LEVEL)

    logging.basicConfig(
        level=MIN_LOG_LEVEL,
        format=LOG_FORMAT,
        handlers=[
            file_handler,
            file_errors_handler,
            console_handler
        ]
    )
