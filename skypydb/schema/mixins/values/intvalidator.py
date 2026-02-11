"""
Module containing the Int64Validator class, which is used by the validator.
"""

from typing import Any
from skypydb.schema.mixins.values import Validator

class Int64Validator(Validator):
    """
    Validator for integer values.
    """

    def validate(
        self,
        value: Any
    ) -> bool:
        """
        Check if value is an integer.
        """

        return isinstance(value, int) and not isinstance(value, bool)

    def __repr__(self) -> str:
        return "value.int64()"
