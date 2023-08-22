#!/usr/bin/env python3

import asyncio
from asyncio.queues import QueueEmpty
from typing import Dict

from ..consumer import Consumer as ABCConsumer
from .message import Message


class Consumer(ABCConsumer):
    def __init__(self, id: str, schema: object, consumers: Dict[str, object]) -> None:
        super().__init__()
        self._queue = asyncio.Queue()
        self.id = id
        self.schema = schema
        self._consumers = consumers
        self._closing = False
        self._receive_task: asyncio.Task = None

    def __aiter__(self):
        return self

    async def __anext__(self):
        """Loop indefinitely until a message is received or the consumer is closed."""
        while not self._closing:
            self._receive_task = asyncio.Task(self._queue.get())
            # Will throw a CancelledError when closing
            message: Message = await self._receive_task
            self._receive_task = None
            # The producer has sent an EOS, let's stop consuming.
            if message is None:
                raise StopAsyncIteration
            return message
        raise StopAsyncIteration

    async def enqueue_async(self, message: Message):
        await self._queue.put(message)

    def ack(self, message: Message):
        # There is no acking in the stub stream
        pass

    def close(self):
        self._closing = True
        del self._consumers[self.id]
        if self._receive_task is not None:
            self._receive_task.cancel()

    def reset(self):
        # Queue has no clear method :(
        try:
            while True:
                self._queue.get_nowait()
        except QueueEmpty:
            pass
