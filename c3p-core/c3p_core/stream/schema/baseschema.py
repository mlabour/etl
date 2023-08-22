#!/usr/bin/env python3

from abc import ABCMeta, abstractmethod


class BaseSchema(metaclass=ABCMeta):
    def __init__(self, record_cls):
        self._record_cls = record_cls

    @abstractmethod
    def encode(self, obj):
        pass

    @abstractmethod
    def decode(self, data):
        pass

    def _validate_object_type(self, obj):
        if not isinstance(obj, self._record_cls):
            raise TypeError('Invalid record obj of type ' + str(type(obj))
                            + ' - expected type is ' + str(self._record_cls))
