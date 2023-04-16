import abc
import json
from typing import Optional
from collections import defaultdict


class BaseStorage:
    """
    An abstract base class for implementing state storage.

    Methods:
        save_state(self, state: dict) -> None:
            Save the given state to a persistent storage.

        retrieve_state(self) -> dict:
            Retrieve the state from a persistent storage and return it as a dictionary.
    """

    @abc.abstractmethod
    def save_state(self, state: dict) -> None:
        """
        Save the given state to a persistent storage.

        Args:
            state (dict): A dictionary representing the state to save.
        """

    @abc.abstractmethod
    def retrieve_state(self) -> dict:
        """
        Retrieve the state from a persistent storage and return it as a dictionary.

        Returns:
            dict: A dictionary representing the retrieved state.
        """


class JsonFileStorage(BaseStorage):
    """
    A storage implementation that uses a JSON file to store and retrieve data.

    Attributes:
        file_path (str): The path to the JSON file.

    Methods:
        __init__(self, file_path: Optional[str] = None):
            Initialize a new instance of the JsonFileStorage class.

        retrieve_state(self) -> dict:
            Load the state from the JSON file, or return an empty dictionary if the file is not found.

        save_state(self, state: dict) -> None:
            Save the given state to the JSON file.
    """

    def __init__(self, file_path: Optional[str] = None):
        self.file_path = file_path

    def retrieve_state(self) -> defaultdict:
        """
        Load the state from the JSON file, or return an empty dictionary if the file is not found.

        Returns:
            dict: A dictionary representing the state loaded from the JSON file,
            or an empty dictionary if the file is not found.
        """
        try:
            with open(self.file_path, 'r') as json_file:
                state = json.load(json_file)
        except FileNotFoundError:
            state = defaultdict(str)

        return state

    def save_state(self, state: dict) -> None:
        """
        Save the given state to the JSON file.

        Args:
            state (dict): A dictionary representing the state to save.
        """
        with open(self.file_path, 'w') as json_file:
            json.dump(state, json_file)

    @classmethod
    def create_storage(cls):
        """
        Create a new instance of the JsonFileStorage class with a default file path.

        Returns:
            JsonFileStorage: A new instance of the JsonFileStorage class.
        """
        json_file_storage = 'state/state_data_storage/json_state_storage.json'
        return JsonFileStorage(json_file_storage)
