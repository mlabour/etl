#!/usr/bin/env python3

from abc import ABCMeta, abstractmethod
from typing import Dict


class Message(metaclass=ABCMeta):
    @property
    @abstractmethod
    def id(self) -> object:
        pass

    @property
    @abstractmethod
    def data(self) -> object:
        pass

    @property
    @abstractmethod
    def value(self) -> object:
        pass

    @property
    @abstractmethod
    def properties(self) -> Dict:
        pass

    @property
    @abstractmethod
    def timestamp(self) -> int:
        pass
