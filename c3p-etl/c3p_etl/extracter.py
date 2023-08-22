#!/usr/bin/env python3
import os
import csv
import shutil

from watchfiles import awatch

import logging
from logging.handlers import RotatingFileHandler

from pathlib import Path

from c3p_core import service
from c3p_core.stream import Producer


logging.basicConfig(
    handlers=[
        RotatingFileHandler(
            "./crisp_extracter.log",
            maxBytes=102400000,
            backupCount=10,  # 100MB
        ),
        logging.StreamHandler(),
    ],
    level=logging.DEBUG,
    format="%(asctime)s %(levelname)s PID_%(process)d %(pathname)s:%(lineno)d %(message)s",
)

logger = logging.getLogger(__name__)


class Extracter(service.Service):
    def __init__(self, producer: Producer = None) -> None:
        self._producer = producer
        self.data_dir = Path(service.ENV.crisp.data_dir)

        self.source_data_dir = self.data_dir / "source"
        self.source_data_dir.mkdir(parents=True, exist_ok=True)

        self.source_bak_data_dir = self.data_dir / "source.bak"
        self.source_bak_data_dir.mkdir(parents=True, exist_ok=True)

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        pass

    async def run(self):
        while True:
            async for changes in awatch(self.source_data_dir):
                for change in changes:
                    # change is a Tuple[Change, str] of change type and file impacted
                    if change[0] == 1:  # File added
                        await self.extract(change[1])

            # Move all the source files to a bak folder
            file_names = os.listdir(self.source_data_dir)
            for file_name in file_names:
                shutil.move(
                    os.path.join(self.source_data_dir, file_name),
                    self.source_bak_data_dir,
                )

    async def read(self, path):
        # Use of generators to iterate over the data lazily
        # without loading the entire data source into memory at once
        # This allows to process large file
        with open(path, "r") as data:
            reader = csv.DictReader(data)
            for row in reader:
                yield row

    async def extract(self, file):
        async for row in self.read(file):
            await self._producer.send_async(
                row, properties={"type": "add", "entity": "OrderRow"}
            )
