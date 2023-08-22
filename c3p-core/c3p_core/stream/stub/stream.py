#!/usr/bin/env python3

from typing import ClassVar, Dict, Tuple

from ..stream import Stream as ABCStream
from .producer import Producer
from .consumer import Consumer


class Stream(ABCStream):

    # Class variable, holds all the topics created and their producer/consumers
    topics:ClassVar[Dict[str, Tuple[Producer, Dict[str, Consumer]]]] = {}

    def __init__(self, topic: str, schema: object) -> None:
        super().__init__()

        self.topic = topic
        self.schema = schema
        endpoints = Stream.topics.get(topic)
        if endpoints is None:
            self._consumers:Dict[str, Consumer] = {}
            self._producer = Producer(self.schema, self._consumers)
            Stream.topics[topic] = (self._producer, self._consumers)
        else:
            self._producer, self._consumers = endpoints

    def create_consumer(self, id:str) -> Consumer:
        consumer = self._consumers.get(id)
        if consumer is None:
            consumer = Consumer(id, self.schema, self._consumers)
            self._consumers[id] = consumer
        return consumer

    def create_producer(self) -> Producer:
        return self._producer

    def close(self):
        pass

    def reset(self):
        self._producer.close()

        consumer:Consumer
        consumers = [ consumer for consumer in self._consumers.values() ]
        for consumer in consumers:
            consumer.reset()

        if self.topic in Stream.topics:
            del Stream.topics[self.topic]

    def get_last_message_id(self) -> object:
        return self._producer.count - 1 if self._producer.count > 0  else None
