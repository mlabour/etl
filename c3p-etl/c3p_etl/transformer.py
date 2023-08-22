#!/usr/bin/env python3
import asyncio
from asyncio import CancelledError

import re
import json

import logging
from logging.handlers import RotatingFileHandler

from pathlib import Path

from datetime import datetime

from c3p_core import service
from c3p_core.stream import Producer, Consumer, Message

from c3p_model.order import Order
from c3p_model.weight_unit import WeightUnit

logging.basicConfig(
    handlers=[
        RotatingFileHandler(
            "./crisp_transformer.log",
            maxBytes=102400000,
            backupCount=10,  # 100MB
        ),
        logging.StreamHandler(),
    ],
    level=logging.DEBUG,
    format="%(asctime)s %(levelname)s PID_%(process)d %(pathname)s:%(lineno)d %(message)s",
)

logger = logging.getLogger(__name__)


def parse_int(value):
    return int(value)


def parse_date(year, month, day):
    return datetime(int(year), int(month), int(day)).strftime(
        "%Y-%m-%d %H:%M:%S"
    )


def parse_str(value):
    return str(value)


def parse_float(value):
    return float(value)


def proper_case(value):
    tokens = value.split(" ")
    return "".join(w[0].upper() + w[1:] for w in tokens)


def add_weight_unit(value):
    return WeightUnit(value.lower())


def convert_to_float_with_two_decimals(string):
    # Use regular expression to extract the numeric part from the string
    numeric_part = re.sub(r"[^0-9.]", "", string)

    # Convert the numeric part to a float with two decimals
    float_value = round(float(numeric_part), 2)
    return float_value


class Transformer(service.Service):
    def __init__(self, consumer: Consumer, producer: Producer):
        super().__init__()

        self._consumer = consumer
        self._producer = producer
        self._transformations = self._load_transformations()

        self._entities = {
            "OrderRow": self._transform_order_row,
            "ProductRow": self._transform_product_row,
        }

        self._counters = {
            entity_name: 0 for entity_name in self._entities.keys()
        }

        self._transform_funcs = {
            "convert_to_float_with_two_decimals": convert_to_float_with_two_decimals
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

    def _load_transformations(self):
        with open(Path(service.ENV.crisp.transformations_file), "r") as f:
            return json.load(f)

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

    async def _transform_order_row(self, row: dict):
        order: Order = Order()
        for transformation in self._transformations:
            if "rename" in transformation:
                source_col = transformation["rename"]["source_column"]
                target_col = transformation["rename"]["target_column"]
                data_type = transformation["rename"]["data_type"]
                if data_type == "int":
                    setattr(order, target_col, parse_int(row[source_col]))
                elif data_type == "str":
                    setattr(order, target_col, parse_str(row[source_col]))
                elif data_type == "float":
                    setattr(order, target_col, parse_float(row[source_col]))
            elif "transform" in transformation:
                source_col = transformation["transform"]["source_column"]
                target_col = transformation["transform"]["target_column"]
                transform_func = transformation["transform"]["func"]
                setattr(
                    order,
                    target_col,
                    self._transform_funcs[transform_func](row[source_col]),
                )
            elif "concatenate_date" in transformation:
                year_col = transformation["concatenate_date"]["year_column"]
                month_col = transformation["concatenate_date"]["month_column"]
                day_col = transformation["concatenate_date"]["day_column"]
                target_col = transformation["concatenate_date"]["target_column"]
                setattr(
                    order,
                    target_col,
                    parse_date(
                        row[year_col],
                        row[month_col],
                        row[day_col],
                    ),
                )
            elif "proper_case" in transformation:
                source_col = transformation["proper_case"]["source_column"]
                target_col = transformation["proper_case"]["target_column"]
                setattr(order, target_col, proper_case(row[source_col]))
            elif "add_weight_value" in transformation:
                target_col = transformation["add_weight_value"]["target_column"]
                value = transformation["add_weight_value"]["value"]
                setattr(order, target_col, add_weight_unit(value))

        await self._producer.send_async(
            order, properties={"type": "add", "entity": "Order"}
        )

    async def _transform_product_row(self, row: dict):
        logger.info(f"TODO. Implement transformation for {row}")
