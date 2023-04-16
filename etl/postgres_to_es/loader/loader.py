from abc import abstractmethod
from typing import Any


class Loader:
    """
    An abstract base class for loading data to a target datasource.

    Attributes:
        connection: The connection object to the target datasource.

    Methods:
        load_data(self) -> None:
            Load data to the target datasource.
    """

    def __init__(self, connection: Any) -> None:
        self.connection = connection

    @abstractmethod
    def load_data(self) -> None:
        """
        Load data to the target datasource.

        This is an abstract method that must be implemented by subclasses.
        """
