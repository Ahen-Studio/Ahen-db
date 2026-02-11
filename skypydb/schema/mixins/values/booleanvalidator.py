"""
Module containing the BooleanValidator class, which is used by the validator.
"""

from typing import Any
from skypydb.schema.mixins.values import Validator

class BooleanValidator(Validator):
    """
    Validator for boolean values.
    """

    def validate(
        self,
        value: Any
    ) -> bool:
        """
        Check if value is a boolean.
        """

        return isinstance(value, bool)

    def __repr__(self) -> str:
        return "value.boolean()"
