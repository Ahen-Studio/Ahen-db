"""
Module containing the AuditTable class, which is used to check table operations in the database.
"""

import sqlite3
from typing import (
    Any,
    Dict,
    List,
    Optional
)
from skypydb.security.validation import InputValidator
from skypydb.errors import TableNotFoundError, ValidationError
from skypydb.database.mixins.reactive.utils import Utils

class AuditTable:
    def __init__(
        self,
        path: Optional[str] = None,
        conn: Optional[sqlite3.Connection] = None
    ):
        if conn is not None:
            self.conn = conn
        elif path is not None:
            self.conn = sqlite3.connect(path, check_same_thread=False)
            self.conn.row_factory = sqlite3.Row
        else:
            raise ValueError("Either path or conn must be provided")

        self.utils = Utils(conn=self.conn)

    def table_exists(
        self,
        table_name: str
    ) -> bool:
        """
        Check if a table exists.
        """

        # validate table name
        try:
            table_name = InputValidator.validate_table_name(table_name)
        except ValidationError:
            return False

        cursor = self.conn.cursor()

        cursor.execute(
            "SELECT name FROM sqlite_master WHERE type='table' AND name=?",
            (table_name,),
        )
        return cursor.fetchone() is not None

    def add_columns_if_needed(
        self,
        table_name: str,
        columns: List[str]
    ) -> None:
        """
        Add columns to a table if they don't exist.

        Args:
            table_name: Name of the table
            columns: List of column names to add
        """

        # validate table name
        table_name = InputValidator.validate_table_name(table_name)

        existing_columns = set(self.get_table_columns(table_name))

        cursor = self.conn.cursor()

        for column in columns:
            # validate column name
            validated_column = InputValidator.validate_column_name(column)
            if validated_column not in existing_columns and validated_column not in ("id", "created_at"):
                cursor.execute(f"ALTER TABLE [{table_name}] ADD COLUMN [{validated_column}] TEXT")

        self.conn.commit()

    def get_table_columns(
        self,
        table_name: str
    ) -> List[str]:
        """
        Get list of column names for a table.
        """

        # validate table name
        table_name = InputValidator.validate_table_name(table_name)

        if not self.table_exists(table_name):
            raise TableNotFoundError(f"Table '{table_name}' not found")

        cursor = self.conn.cursor()

        cursor.execute(f"PRAGMA table_info([{table_name}])")
        return [row[1] for row in cursor.fetchall()]

    def check_config_table(self) -> None:
        """
        Create the system table for storing table configurations if it doesn't exist.
        """

        cursor = self.conn.cursor()

        cursor.execute(
            """
            CREATE TABLE IF NOT EXISTS _skypy_config (
                table_name TEXT PRIMARY KEY,
                config TEXT NOT NULL,
                created_at TEXT NOT NULL
            )
            """
        )
        self.conn.commit()

    def validate_data_with_config(
        self,
        table_name: str,
        data: Dict[str, Any]
    ) -> Dict[str, Any]:
        """
        Validate data against the table's configuration, converts values to the correct type based on configuration.

        Args:
            table_name: Name of the table
            data: Data dictionary to validate

        Returns:
            Validated data dictionary with converted values

        Raises:
            ValueError: If data validation fails
        """

        config = self.utils.get_table_config(table_name)
        if not config:
            # no configuration, return data as-is
            return data

        validated_data = {}

        for key, value in data.items():
            if key in config:
                expected_type = config[key]
                optional = False
                if isinstance(expected_type, dict):
                    optional = expected_type.get("optional", False)
                    expected_type = expected_type.get("type", "str")

                if value is None and optional:
                    validated_data[key] = None
                    continue
                # skip "auto" type
                if expected_type == "auto" or expected_type == "id":
                    continue
                # type conversion and validation
                if expected_type is str or expected_type == "str":
                    validated_data[key] = str(value)
                elif expected_type is int or expected_type == "int":
                    try:
                        validated_data[key] = int(value)
                    except (ValueError, TypeError):
                        raise ValueError(
                            f"Invalid type for column '{key}': expected int"
                        )
                elif expected_type is float or expected_type == "float":
                    try:
                        validated_data[key] = float(value)
                    except (ValueError, TypeError):
                        raise ValueError(
                            f"Invalid type for column '{key}': expected float"
                        )
                elif expected_type is bool or expected_type == "bool":
                    if isinstance(value, str):
                        validated_data[key] = value.lower() in ("true", "1", "yes")
                    else:
                        validated_data[key] = bool(value)
                else:
                    # unknown type, store as string
                    validated_data[key] = str(value)
            else:
                # column not in config, store as-is
                validated_data[key] = value
        return validated_data
