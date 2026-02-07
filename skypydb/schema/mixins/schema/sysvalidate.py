"""
Module containing the SysValidate class, which is used to validate a row of data against a table definition.
"""

from typing import (
    Dict,
    Any
)
from skypydb.schema.values import Validator

class SysValidate:
    def __init__(
        self,
        columns: Dict[str, Validator]
    ):
        self.columns = columns

    def validate_row(
        self,
        row_data: Dict[str, Any]
    ) -> None:
        """
        Validate a row of data against this table definition.

        Args:
            row_data: Dictionary of column names to values

        Raises:
            ValueError: If validation fails or required columns are missing
        """

        # check all required columns are present
        for col_name, validator in self.columns.items():
            if col_name not in row_data:
                # check if the column is optional or required
                is_optional = getattr(validator, 'optional', False)
                if is_optional:
                    continue
                raise ValueError(
                    f"Missing required column: '{col_name}'"
                )
            value = row_data[col_name]
            if not validator.validate(value):
                raise ValueError(
                    f"Invalid value for column '{col_name}': "
                    f"expected {validator}, got {type(value).__name__}"
                )
