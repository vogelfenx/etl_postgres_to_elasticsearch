from .persistent_state_manager import BaseStorage, JsonFileStorage # кажется, JsonFileStorage тут не нумно импортировать
from typing import Any


class State:
    """
    A class for storing state when working with data, to avoid re-reading data from the beginning.

    Methods:
        set_state(key: str, value: Any) -> None:
            Set the state for a given key, and save the updated state to the storage.
        get_state(key: str) -> Any:
            Get the state for a given key. If the key is not found, return None.
    """

    def __init__(self, storage: BaseStorage):
        self.storage = storage
        self.state = storage.retrieve_state()

    def set_state(self, key: str, value: Any) -> None:
        """
        Set the state for a given key, and save the updated state to the storage.

        Args:
            key (str): The key to set the state for.
            value (Any): The state value to set.
        """
        self.state[key] = value
        self.storage.save_state(self.state)

    def get_state(self, key: str) -> Any:
        """
        Get the state for a given key. If the key is not found, return None.

        Args:
            key (str): The key to get the state for.

        Returns:
            Any: The state value for the given key, or None if the key is not found.
        """
        state = self.state.get(key, None)
        return state
