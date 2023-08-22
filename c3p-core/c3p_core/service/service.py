#!/usr/bin/env python3

from abc import ABC, abstractmethod


class Service(ABC):
    def __init__(self):
        pass

    @abstractmethod
    async def run(self):
        pass
