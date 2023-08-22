#!/usr/bin/env python3

from abc import ABCMeta, abstractmethod

from .producer import Producer
from .consumer import Consumer


class Stream(metaclass=ABCMeta):
    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.close()

    @abstractmethod
    def create_consumer(self, id: str) -> Consumer:
        pass

    @abstractmethod
    def create_producer(self) -> Producer:
        pass

    @abstractmethod
    def close(self):
        pass

    @abstractmethod
    def reset(self):
        pass

    @abstractmethod
    def get_last_message_id(self) -> object:
        pass
