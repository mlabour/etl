#!/usr/bin/env python3

from typing import Dict
from ..message import Message as ABCMessage


class Message(ABCMessage):
    def __init__(
        self,
        id: object,
        data: object,
        schema: object,
        properties: Dict = None,
        timestamp: int = None,
    ) -> None:
        self._id = id
        self._data = data
        self._schema = schema
        self._value = None
        self._properties = properties
        self._timestamp = timestamp

    @property
    def id(self) -> object:
        return self._id

    @property
    def data(self) -> object:
        return self._data

    @property
    def value(self) -> object:
        # Cache the decoding of the data
        if self._value is None and self._data is not None:
            self._value = self._schema.decode(self._data)
        return self._value

    @property
    def properties(self) -> Dict:
        return self._properties

    @property
    def timestamp(self) -> int:
        return self._timestamp

    def __str__(self) -> str:
        return str(self._id)
