from enum import Enum, auto


class Type(Enum):
    """Type of stream."""

    NONE = auto()
    STUB = auto()

    def __str__(self):
        return self.name

    @staticmethod
    def from_string(s):
        try:
            return Type[s]
        except KeyError:
            raise ValueError()
