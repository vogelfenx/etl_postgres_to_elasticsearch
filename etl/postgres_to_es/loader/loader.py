from abc import abstractmethod


class Loader:
    """Abstract class Loader."""

    def __init__(self, connection):
        self.connection = connection

    def load_data(self):
        """Load data to target datasource"""
