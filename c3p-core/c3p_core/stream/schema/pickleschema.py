#!/usr/bin/env python3

import pickle

from .baseschema import BaseSchema


class PickleSchema(BaseSchema):
    """A schema based on the Pickle module."""

    def __init__(self, record_cls) -> None:
        super().__init__(record_cls)

    def encode(self, obj):
        if obj is not None:
            self._validate_object_type(obj)
        return pickle.dumps(obj)

    def decode(self, data):
        return pickle.loads(data)
