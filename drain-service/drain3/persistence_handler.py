"""
Adopted from https://github.com/IBM/Drain3
"""
# Standard Library
from abc import ABC, abstractmethod


class PersistenceHandler(ABC):
    @abstractmethod
    def save_state(self, state):
        pass

    @abstractmethod
    def load_state(self):
        pass
