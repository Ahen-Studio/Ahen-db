"""
Module containing the SysValidation class, which is used to validate data in the database.
"""

from skypydb.errors import ValidationError
from typing import (
    Optional,
    Dict,
    Any
)
from skypydb.security.constants import (
    MAX_TABLE_NAME_LENGTH,
    MAX_COLUMN_NAME_LENGTH,
    MAX_STRING_LENGTH,
    TABLE_NAME_PATTERN,
    COLUMN_NAME_PATTERN
)
from skypydb.security.mixins.validation.syscheck import SysCheck
from skypydb.security.mixins.validation.syssanitize import SysSanitize

class SysValidation:
    @classmethod
    def __init__(
        cls,
        sys_check: "SysCheck",
        sys_sanitize: "SysSanitize"
    ):
        cls.sys_check = sys_check
        cls.sys_sanitize = sys_sanitize

    @classmethod
    def validate_table_name(
        cls,
        table_name: str
    ) -> str:
        """
        Validate a table name.

        Args:
            table_name: Name of the table to validate

        Returns:
            Validated table name

        Raises:
            ValidationError: If table name is invalid
        """

        if not table_name:
            raise ValidationError("Table name cannot be empty")
        if not isinstance(table_name, str):
            raise ValidationError("Table name must be a string")
        if len(table_name) > MAX_TABLE_NAME_LENGTH:
            raise ValidationError(
                f"Table name too long (max {MAX_TABLE_NAME_LENGTH} characters)"
            )
        if not TABLE_NAME_PATTERN.match(table_name):
            raise ValidationError(
                "Table name must start with a letter or underscore and contain only "
                "alphanumeric characters, underscores, and hyphens"
            )
        # check for SQL injection patterns
        if cls._contains_sql_injection(table_name):
            raise ValidationError("Table name contains potentially dangerous characters")
        return table_name

    @classmethod
    def validate_column_name(
        cls,
        column_name: str
    ) -> str:
        """
        Validate a column name.

        Args:
            column_name: Name of the column to validate

        Returns:
            Validated column name

        Raises:
            ValidationError: If column name is invalid
        """

        if not column_name:
            raise ValidationError("Column name cannot be empty")
        if not isinstance(column_name, str):
            raise ValidationError("Column name must be a string")
        if len(column_name) > MAX_COLUMN_NAME_LENGTH:
            raise ValidationError(
                f"Column name too long (max {MAX_COLUMN_NAME_LENGTH} characters)"
            )
        if not COLUMN_NAME_PATTERN.match(column_name):
            raise ValidationError(
                "Column name must start with a letter or underscore and contain only "
                "alphanumeric characters and underscores"
            )
        # check for SQL injection patterns
        if cls._contains_sql_injection(column_name):
            raise ValidationError("Column name contains potentially dangerous characters")
        return column_name

    @classmethod
    def validate_string_value(
        cls,
        value: str,
        max_length: Optional[int] = None
    ) -> str:
        """
        Validate a string value.

        Args:
            value: String value to validate
            max_length: Optional maximum length (defaults to MAX_STRING_LENGTH)

        Returns:
            Validated string value

        Raises:
            ValidationError: If string value is invalid
        """

        if not isinstance(value, str):
            raise ValidationError("Value must be a string")

        max_len = max_length or MAX_STRING_LENGTH

        if len(value) > max_len:
            raise ValidationError(
                f"String value too long (max {max_len} characters)"
            )
        return value

    @classmethod
    def validate_data_dict(
        cls,
        data: Dict[str, Any]
    ) -> Dict[str, Any]:
        """
        Validate a dictionary of data.

        Args:
            data: Dictionary containing data to validate

        Returns:
            Validated data dictionary

        Raises:
            ValidationError: If data is invalid
        """

        if not isinstance(data, dict):
            raise ValidationError("Data must be a dictionary")

        validated_data = {}

        for key, value in data.items():
            # validate column name
            validated_key = cls.validate_column_name(key)

            # validate value based on type
            if isinstance(value, str):
                validated_value = cls.sanitize_string(value)
            elif isinstance(value, (int, float, bool)):
                validated_value = value
            elif value is None:
                validated_value = None
            else:
                # convert to string for other types
                validated_value = cls.sanitize_string(str(value))

            validated_data[validated_key] = validated_value
        return validated_data

    @classmethod
    def validate_filter_dict(
        cls,
        filters: Dict[str, Any]
    ) -> Dict[str, Any]:
        """
        Validate filter parameters for search/delete operations.

        Args:
            filters: Dictionary containing filter parameters

        Returns:
            Validated filter dictionary

        Raises:
            ValidationError: If filters are invalid
        """

        if not isinstance(filters, dict):
            raise ValidationError("Filters must be a dictionary")

        validated_filters = {}

        for key, value in filters.items():
            # validate column name
            validated_key = cls.validate_column_name(key)
            # validate value(s)
            if isinstance(value, list):
                validated_value = [
                    cls.sanitize_string(str(v)) if not isinstance(v, (int, float, bool, type(None)))
                    else v
                    for v in value
                ]
            elif isinstance(value, str):
                validated_value = cls.sanitize_string(value)
            elif isinstance(value, (int, float, bool)):
                validated_value = value
            elif value is None:
                validated_value = None
            else:
                validated_value = cls.sanitize_string(str(value))

            validated_filters[validated_key] = validated_value
        return validated_filters

    @classmethod
    def validate_config(
        cls,
        config: Dict[str, Any]
    ) -> Dict[str, Any]:
        """
        Validate a table configuration dictionary.

        Args:
            config: Configuration dictionary

        Returns:
            Validated configuration

        Raises:
            ValidationError: If configuration is invalid
        """

        if not isinstance(config, dict):
            raise ValidationError("Configuration must be a dictionary")

        validated_config = {}

        for table_name, table_config in config.items():
            # validate table name
            validated_table_name = cls.validate_table_name(table_name)
            if not isinstance(table_config, dict):
                raise ValidationError(
                    f"Configuration for table '{table_name}' must be a dictionary"
                )
            validated_table_config = {}
            for column_name, column_type in table_config.items():
                # validate column name
                validated_column_name = cls.validate_column_name(column_name)
                # validate column type
                valid_types = [str, int, float, bool, "str", "int", "float", "bool", "auto"]

                if column_type not in valid_types:
                    raise ValidationError(
                        f"Invalid type for column '{column_name}': {column_type}. "
                        f"Valid types are: {valid_types}"
                    )
                validated_table_config[validated_column_name] = column_type
            validated_config[validated_table_name] = validated_table_config
        return validated_config

def validate_table_name(
    table_name: str,
    sys_validation: "SysValidation"
) -> str:
    """
    Convenience function to validate a table name.

    Args:
        table_name: Name to validate
        sys_validation: SysValidation instance to use for validation

    Returns:
        Validated table name
    """

    return sys_validation.validate_table_name(table_name)

def validate_column_name(
    column_name: str,
    sys_validation: "SysValidation"
) -> str:
    """
    Convenience function to validate a column name.

    Args:
        column_name: Name to validate
        sys_validation: SysValidation instance to use for validation

    Returns:
        Validated column name
    """

    return sys_validation.validate_column_name(column_name)
