#!/usr/bin/env python3
import argparse

import csv
import random
import locale
import string
import shutil
import time

import logging
from pathlib import Path
from logging.handlers import RotatingFileHandler


logging.basicConfig(
    handlers=[
        RotatingFileHandler(
            "./create_customer_files.log",
            maxBytes=102400000,
            backupCount=10,  # 100MB
        ),
        logging.StreamHandler(),
    ],
    level=logging.DEBUG,
    format="%(asctime)s %(levelname)s PID_%(process)d %(pathname)s:%(lineno)d %(message)s",
)


description = """
Watch for incoming order files, transform them
and publish them as Order entities.
"""

parser = argparse.ArgumentParser(
    description=description, formatter_class=argparse.RawTextHelpFormatter
)

parser.add_argument(
    "--nb-rows",
    default=10,
    help="Rows to process",
    required=False,
)

parser.add_argument(
    "--interval",
    default=0,
    help="Interval in second between 2 executions. If 0, then execute only once",
    required=False,
)

args = parser.parse_args()


def generate_random_amount():
    # Set the locale to get the desired formatting with commas
    locale.setlocale(locale.LC_ALL, "en_US.UTF8")

    # Generate a random float between 1000 and 10000
    random_float = random.uniform(1000, 10000)

    # Format the number with commas
    formatted_amount = locale.format_string(
        "%0.2f", random_float, grouping=True
    )

    return formatted_amount


def create_customer_csv_file(nb_rows: int):
    s = string.ascii_lowercase
    c = "".join(random.choice(s) for i in range(8))
    filename = f"order_{c}.csv"
    file_dir = Path("../data/tmp")
    file_dir.mkdir(parents=True, exist_ok=True)
    file_path = file_dir / Path(filename)

    header = [
        "Order Number",
        "Year",
        "Month",
        "Day",
        "Product Number",
        "Product Name",
        "Count",
        "Extra Col1",
        "Extra Col2",
        "Empty Column",
    ]

    with open(file_path, "w", newline="") as file:
        writer = csv.writer(file)
        writer.writerow(header)
        nb = random.randint(0, 10000)
        for i in range(nb_rows):
            random_order_number = nb + i
            random_year = random.randint(2010, 2023)
            random_month = random.randint(1, 12)
            random_day = random.randint(1, 28)
            random_product_number = f"P-{random.randint(10001,20000)}"
            random_product_name = "this is the product name {0} {1}".format(
                "".join(random.choice(s) for i in range(8)),
                "".join(random.choice(s) for i in range(8)),
            )
            random_count = generate_random_amount()
            random_extra_col1 = "".join(random.choice(s) for i in range(8))
            random_extra_col2 = "".join(random.choice(s) for i in range(8))
            empty_col = ""

            writer.writerow(
                [
                    random_order_number,
                    random_year,
                    random_month,
                    random_day,
                    random_product_number,
                    random_product_name,
                    random_count,
                    random_extra_col1,
                    random_extra_col2,
                    empty_col,
                ]
            )

    destination_dir = Path("../data/source")
    destination_dir.mkdir(parents=True, exist_ok=True)
    destination_path = destination_dir / Path(filename)
    shutil.move(str(file_path), destination_path)


def repeat_function(interval, nb_rows):
    while True:
        create_customer_csv_file(nb_rows)
        if interval == 0:
            break
        time.sleep(interval)


if __name__ == "__main__":
    repeat_function(args.interval, args.nb_rows)
