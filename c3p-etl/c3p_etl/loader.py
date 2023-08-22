#!/usr/bin/env python3
from enum import Enum

import asyncio
import aiofiles
import dataclasses

from asyncio import CancelledError

import logging
import csv

from logging.handlers import RotatingFileHandler
from pathlib import Path

from c3p_core.stream import Consumer, Message

from c3p_core import service

logging.basicConfig(
    handlers=[
        RotatingFileHandler(
            "./crisp_loader.log", maxBytes=102400000, backupCount=10  # 100MB
        ),
        logging.StreamHandler(),
    ],
    level=logging.DEBUG,
    format="%(asctime)s %(levelname)s PID_%(process)d %(pathname)s:%(lineno)d %(message)s",
)

logger = logging.getLogger(__name__)


class Loader(service.Service):
    def __init__(self, consumer: Consumer):
        super().__init__()

        self.data_dir = Path(service.ENV.crisp.data_dir)
        self.target_data_dir = self.data_dir / "target"
        self.target_data_dir.mkdir(parents=True, exist_ok=True)

        self._consumer = consumer

        self._entities = {
            "Order": self._load_order,
            "Product": self._load_product,
        }

        self._counters = {
            entity_name: 0 for entity_name in self._entities.keys()
        }

    async def _log_health_check(self):
        while True:
            await asyncio.sleep(3)
            for entity_name, counter in self._counters.items():
                if counter:
                    logger.debug(
                        f"Received since last check: {counter} {entity_name}"
                    )
                    self._counters[entity_name] = 0

    async def run(self):
        asyncio.ensure_future(self._log_health_check())

        msg: Message
        async for msg in self._consumer:
            try:
                props = msg.properties
                msg_type = props.get("type")

                if msg_type == "add":
                    entity_type = props.get("entity")
                    if entity_type not in self._entities.keys():
                        logger.warn(
                            f"Received unknown entity type: {entity_type}"
                        )
                        continue

                    self._counters[entity_type] += 1
                    await self._entities[entity_type](msg.value)

                elif msg_type == "end_of_stream":
                    return

                else:
                    logger.warning(
                        f"Received unsupported message type: {msg_type}"
                    )

            except Exception as ex:
                if isinstance(ex, CancelledError):
                    raise
                else:
                    logger.error(ex)

    async def _load_order(self, instance_of_entity):
        await self.write_dict_to_csv(self.target_data_dir, instance_of_entity)

    async def _load_product(self, instance_of_entity):
        # TODO: Implement when there is need to process product files
        pass

    async def write_dict_to_csv(self, dir_path, instance_of_entity):
        # We assume that if the file exists, it has the correct headers
        # Otherwise we write the headers
        # A TODO would be to account for the fact that a file can exists and have no header

        file_path = dir_path / "order.csv"

        write_header = False
        if not file_path.exists():
            write_header = True

        async with aiofiles.open(file_path, "a", newline="") as file:
            fieldnames = [
                field.name for field in dataclasses.fields(instance_of_entity)
            ]
            writer = csv.DictWriter(file, fieldnames=fieldnames)
            if write_header:
                await writer.writeheader()
            values = [
                getattr(instance_of_entity, field.name)
                for field in dataclasses.fields(instance_of_entity)
            ]

            values = list(
                map(lambda x: x.value if isinstance(x, Enum) else x, values)
            )

            await writer.writerow(dict(zip(fieldnames, values)))
