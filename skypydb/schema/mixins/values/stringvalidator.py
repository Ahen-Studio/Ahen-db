"""
Module containing the StringValidator class, which is used by the validator.
"""

from typing import Any
from skypydb.schema.mixins.values import Validator

class StringValidator(Validator):
    """
    Validator for string values.
    """

    def validate(
        self,
        value: Any
    ) -> bool:
        """
        Check if value is a string.
        """

        return isinstance(value, str)

    def __repr__(self) -> str:
        return "value.string()"
