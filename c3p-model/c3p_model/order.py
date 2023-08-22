#!/usr/bin/env python3
import dataclasses

from datetime import datetime

from .weight_unit import WeightUnit


@dataclasses.dataclass
class Order:
    OrderID: int = 0
    OrderDate: datetime = None
    ProductId: str = ""
    ProductName: str = ""
    Quantity: float = 0.0
    Unit: WeightUnit = WeightUnit.Unknown
