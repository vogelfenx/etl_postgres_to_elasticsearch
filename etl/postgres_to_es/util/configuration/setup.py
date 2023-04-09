from util.logger import Logger
from pathlib import Path
import re


def setup_logger():
    log_filepath = 'logs/.log'  # TODO: place it in config file

    filepath_parent_directories = re.split('[\\\/]', log_filepath)[:-1]
    filepath_parent_directories = Path('/'.join(filepath_parent_directories))

    Path.mkdir(filepath_parent_directories, parents=True, exist_ok=True)

    Logger(log_filepath)


def setup():
    setup_logger()
