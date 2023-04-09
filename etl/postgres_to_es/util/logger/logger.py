import logging


class Logger:
    """Helper class for logger setup."""

    def __init__(self, log_filepath: str, message_format: str = None):
        """Initialize the Logger.

        Args:
            log_filepath (str): The file path where the log is written.
            message_format (str, optional): format of the log entry. Defaults to None.
        """
        if not message_format:
            message_format = '%(asctime)s %(message)s'

        logging.basicConfig(filename=log_filepath, format=message_format)
