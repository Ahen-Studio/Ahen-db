"""
Module containing the Float64Validator class, which is used by the validator.
"""

from typing import Any
from skypydb.schema.mixins.values import Validator

class Float64Validator(Validator):
    """
    Validator for float values.
    """

    def validate(
        self,
        value: Any
    ) -> bool:
        """
        Check if value is a float or integer.
        """

        return isinstance(value, (int, float)) and not isinstance(value, bool)

    def __repr__(self) -> str:
        return "value.float64()"
