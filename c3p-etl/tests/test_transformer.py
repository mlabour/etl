#!/usr/bin/env python3

from pathlib import Path
import unittest
import sqlite3
import json
import re
import datetime

from c3p_etl.transformer import convert_to_float_with_two_decimals


class TestCache(unittest.TestCase):
    def test_convert_to_float_with_two_decimals(self):
        self.assertEqual(
            convert_to_float_with_two_decimals("5,234.05"), 5234.05
        )


if __name__ == "__main__":
    unittest.main()
