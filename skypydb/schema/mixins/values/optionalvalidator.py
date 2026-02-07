"""
Module containing the OptionalValidator class, which is used to validate optional values.
"""

from typing import Any
from skypydb.schema.mixins.values import Validator

class OptionalValidator(Validator):
    """
    Validator for optional values (can be None or the wrapped type).
    """

    def __init__(
        self,
        validator: Validator
    ):
        """
        Initialize optional validator.

        Args:
            validator: The validator for the non-null type
        """

        self.validator = validator
        self.optional = True

    def validate(
        self,
        value: Any
    ) -> bool:
        """
        Check if value is None or valid according to wrapped validator.
        """

        if value is None:
            return True
        return self.validator.validate(value)

    def __repr__(self) -> str:
        return f"v.optional({self.validator})"
