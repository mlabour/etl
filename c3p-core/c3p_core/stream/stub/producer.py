#!/usr/bin/env python3

import asyncio
from typing import Dict, Tuple

from ..producer import Producer as ABCProducer
from .consumer import Consumer
from .message import Message


class Producer(ABCProducer):
    def __init__(self, schema: object, consumers: Dict[str, Consumer]) -> None:
        super().__init__()
        self._loop = asyncio.get_event_loop()
        self.schema = schema
        self._consumers = consumers
        self.count = 0

    def _prepare(self, value, properties, timestamp) -> Tuple[Message, str]:
        data = self.schema.encode(value)
        message = Message(
            self.count,
            data,
            schema=self.schema,
            properties=properties,
            timestamp=timestamp,
        )
        self.count += 1
        return message

    async def send_async(self, value, properties: Dict = None, timestamp: int = None) -> None:
        message = self._prepare(value, properties=properties, timestamp=timestamp)
        for consumer in self._consumers.values():
            await consumer.enqueue_async(message)
        # Yield to other tasks
        # When you use await asyncio.sleep(0),
        # you are essentially introducing a very short delay of zero seconds.
        # It might seem counterintuitive, but this can be a useful technique in certain situations.
        # such as yielding control to the Event Loop: Even though the delay is zero seconds,
        # the await asyncio.sleep(0) call gives the event loop an opportunity to switch to other
        # tasks that might be waiting to execute. This can help prevent long-running coroutines
        # from monopolizing the event loop and improve responsiveness in situations where tasks are competing for execution time.
        await asyncio.sleep(0)

    def close(self):
        # Send an empty message as an end-of-stream signal.
        # Issue: If the consumer starts after the producer has closed, it won't return.
        for consumer in self._consumers.values():
            asyncio.ensure_future(consumer.enqueue_async(None))
