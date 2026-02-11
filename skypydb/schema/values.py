"""
Type validators for Skypydb schema.
"""

from skypydb.schema.mixins.values import (
    Validator,
    StringValidator,
    Int64Validator,
    Float64Validator,
    BooleanValidator,
    OptionalValidator
)

class Values:
    """
    Factory for creating type validators.
    """

    # create a string validator
    @staticmethod
    def string() -> Validator:
        """
        Create a string validator.
        """

        return StringValidator()


    # create an integer validator
    @staticmethod
    def int64() -> Validator:
        """
        Create an integer validator.
        """

        return Int64Validator()

    # create a float validator
    @staticmethod
    def float64() -> Validator:
        """
        Create a float validator.
        """

        return Float64Validator()

    # create a boolean validator
    @staticmethod
    def boolean() -> Validator:
        """
        Create a boolean validator.
        """

        return BooleanValidator()

    # create an optional validator
    @staticmethod
    def optional(validator: Validator) -> Validator:
        """
        Create an optional validator.

        Args:
            validator: The validator to make optional

        Returns:
            An optional validator
        """

        return OptionalValidator(validator)

# create singleton instance for easy import
value = Values()
