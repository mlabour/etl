#!/usr/bin/env python3

import asyncio
from typing import Callable
import unittest

from c3p_core.stream import Stream, Consumer, Message
from c3p_core.stream.schema import PickleSchema
import c3p_core.stream.stub as sstream


TOPIC = "MyTopic"

PAYLOAD_FORMAT = "This is message %d"


async def produce_async(stream_factory: Callable[[str], Stream], n: int):
    with stream_factory() as stream:
        with stream.create_producer() as producer:
            # Gives the consumer a chance to start
            await asyncio.sleep(0.001)
            for i in range(n):
                msg = PAYLOAD_FORMAT % i
                producer.send(msg, key=str(i), properties={"type": "update"})
                print(f"Produced: {msg}")


async def consume_async(tester: "TestStream", id: str, stream_factory: Callable[[str], Stream], n: int):
    with stream_factory() as stream:
        with stream.create_consumer(id) as consumer:
            i = 0
            message: Message
            async for message in consumer:
                print(f"Consumed (id={consumer.id}): {message.id}")
                tester.assertEqual(i, int(message.key))
                tester.assertEqual(PAYLOAD_FORMAT % i, message.value)
                consumer.ack(message)
                i += 1
                if i == n:
                    return


async def consume_thru_failures(tester: "TestStream", id: str, stream_factory: Callable[[str], Stream], n: int):
    failed = False
    i = 0
    with stream_factory() as stream:
        while True:
            try:
                with stream.create_consumer(id) as consumer:
                    async for message in consumer:
                        print(f"Consumed (id={consumer.id}): {message.id}")
                        tester.assertEqual(i, int(message.key))
                        tester.assertEqual(PAYLOAD_FORMAT % i, message.value)
                        if not failed and i > n / 2:
                            failed = True
                            raise RuntimeError("Consumer failure")
                        consumer.ack(message)
                        i += 1
                        if i == n:
                            return
            except RuntimeError as e:
                print(e)


async def consume_with_consumer(stream: Stream, consumer: Consumer):
    last_msg_id = stream.get_last_message_id()
    if last_msg_id is None:
        raise EOFError("No message has been published yet")

    count = 0
    async for message in consumer:
        msg_id = message.id

        print(f"Consumed (id={consumer.id}): {message.id}")
        consumer.ack(message)
        count += 1

        if msg_id == last_msg_id:
            return count


class TestStream(unittest.TestCase):
    def test_sync_producer(self):
        sstream.Stream(TOPIC, PickleSchema(str)).reset()

        num = 10
        loop = asyncio.get_event_loop()
        with sstream.Stream(TOPIC, PickleSchema(str)) as stream:
            with stream.create_producer() as producer, stream.create_consumer("S") as consumer:
                for i in range(num):
                    msg = PAYLOAD_FORMAT % i
                    producer.send(msg, key=str(i), properties={"type": "update"})
                    print(f"Produced: {msg}")
                    self.assertEqual(i, stream.get_last_message_id())

                count = loop.run_until_complete(consume_with_consumer(stream, consumer))

        self.assertEqual(num, count)

    def test_produce_and_consume_messages(self):
        sstream.Stream(TOPIC, PickleSchema(str)).reset()

        num = 10
        factory = lambda: sstream.Stream(TOPIC, PickleSchema(str))
        loop = asyncio.get_event_loop()
        loop.run_until_complete(asyncio.gather(produce_async(factory, num), consume_async(self, "1", factory, num)))

    def test_multiple_consumers(self):
        sstream.Stream(TOPIC, PickleSchema(str)).reset()

        num = 10
        factory = lambda: sstream.Stream(TOPIC, PickleSchema(str))
        loop = asyncio.get_event_loop()
        loop.run_until_complete(
            asyncio.gather(
                produce_async(factory, num),
                consume_async(self, "A", factory, num),
                consume_async(self, "B", factory, num),
            )
        )

    @unittest.skip("Acking not implemented and in-memory queue is deleted.")
    def test_stream_failure(self):
        sstream.Stream(TOPIC, PickleSchema(str)).reset()

        num = 10
        factory = lambda: sstream.Stream(TOPIC, PickleSchema(str))
        loop = asyncio.get_event_loop()
        loop.run_until_complete(
            asyncio.gather(
                produce_async(factory, num),
                consume_thru_failures(self, "F", factory, num),
            )
        )


if __name__ == "__main__":
    unittest.main()
