#!/usr/bin/env python3

import argparse

from c3p_core.stream.schema import PickleSchema

from c3p_core import service

import c3p_core.stream.stub as sstream

from c3p_etl.extracter import Extracter
from c3p_etl.transformer import Transformer
from c3p_etl.loader import Loader

from c3p_model.order import Order


description = """
Watch for incoming order files, transform them
and publish them as Order entities.
"""

parser = argparse.ArgumentParser(
    description=description, formatter_class=argparse.RawTextHelpFormatter
)

parser.add_argument(
    "--config-file",
    default="../crisp.yml",
    help="Location of the config file",
    required=False,
)

args = parser.parse_args()

service.load_configuration(args.config_file)

e2t_stream = None
e2t_producer = e2t_consumer = None
t2l_stream = None
t2l_producer = t2l_consumer = None
try:
    # Create the extracter with its output stream
    e2t_stream = sstream.Stream(
        topic="OrderRow",
        schema=PickleSchema(dict),
    )
    e2t_producer = e2t_stream.create_producer()

    extracter = Extracter(producer=e2t_producer)
    service.SERVICES.register(extracter)

    # Create the transformer
    e2t_consumer = e2t_stream.create_consumer("transformer")

    t2l_stream = sstream.Stream(
        topic=Order.__name__.lower(),
        schema=PickleSchema(Order),
    )
    t2l_producer = t2l_stream.create_producer()

    transformer = Transformer(e2t_consumer, t2l_producer)
    service.SERVICES.register(transformer)

    # Create the loader
    t2l_consumer = t2l_stream.create_consumer("loader")

    loader = Loader(t2l_consumer)
    service.SERVICES.register(loader)

    # Start the services
    service.SERVICES.start()

finally:
    if e2t_producer is not None:
        e2t_producer.close()

    if e2t_consumer is not None:
        e2t_consumer.close()

    if e2t_stream is not None:
        e2t_stream.close()

    if t2l_producer is not None:
        t2l_producer.close()

    if t2l_consumer is not None:
        t2l_consumer.close()

    if t2l_stream is not None:
        t2l_stream.close()
