#!/usr/bin/env python3

from abc import ABCMeta, abstractmethod
from typing import Dict


class Producer(metaclass=ABCMeta):
    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.close()

    @abstractmethod
    async def send_async(self, value, key: str = None, properties: Dict = None, timestamp: int = None) -> None:
        pass

    @abstractmethod
    def close(self):
        pass
