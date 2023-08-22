#!/usr/bin/env python3

from abc import ABCMeta, abstractmethod

from .message import Message

class Consumer(metaclass=ABCMeta):
    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.close()

    @abstractmethod
    def __aiter__(self):
        pass

    @abstractmethod
    async def __anext__(self):
        pass

    @abstractmethod
    def ack(self, message: Message):
        '''TODO: make async'''
        pass

    @abstractmethod
    def close(self):
        pass
