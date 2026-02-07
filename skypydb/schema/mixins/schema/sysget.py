"""
Module containing the SysGet class, which is used to get SQL index.
"""

from typing import (
    List,
    Dict,
    Any,
    Optional
)
from skypydb.schema.values import (
    Validator,
    Int64Validator,
    Float64Validator,
    BooleanValidator
)

class SysGet:
    def __init__(
        self,
        columns: Dict[str, Validator],
        table_name: Optional[str] = None
    ):
        self.columns = columns
        self.indexes: List[Dict[str, Any]] = []
        self.table_name = table_name

    def get_sql_columns(self) -> List[str]:
        """
        Produce SQL column definitions for creating the table.

        The returned list:
        - Always includes the required columns: "id TEXT PRIMARY KEY" and
          "created_at TEXT NOT NULL".
        - For each column in the table definition (except "id" and "created_at"),
          maps the column's validator to an SQL type:
            * `Int64Validator` and `BooleanValidator` -> `INTEGER`
            * `Float64Validator` -> `REAL`
            * all other validators -> `TEXT`
        - Wraps column names in square brackets in the returned definitions.

        Returns:
            List[str]: SQL column definition strings suitable for a CREATE TABLE
            statement (e.g. "[name] TEXT", "age INTEGER").
        """

        sql_columns = [
            "id TEXT PRIMARY KEY",
            "created_at TEXT NOT NULL"
        ]

        for col_name, validator in self.columns.items():
            base_validator = getattr(validator, "validator", validator)
            if col_name in ["id", "created_at"]:
                continue
            # map validators to SQL types
            if isinstance(base_validator, Int64Validator):
                sql_type = "INTEGER"
            elif isinstance(base_validator, Float64Validator):
                sql_type = "REAL"
            elif isinstance(base_validator, BooleanValidator):
                sql_type = "INTEGER"
            else:
                sql_type = "TEXT"  # default for strings and other types

            sql_columns.append(f"[{col_name}] {sql_type}")
        return sql_columns

    def get_sql_indexes(self) -> List[str]:
        """
        Get SQL index creation statements for this table.

        Returns:
            List of SQL CREATE INDEX statements
        """

        if not self.table_name:
            return []

        sql_indexes = []
        for index_def in self.indexes:
            index_name = f"idx_{self.table_name}_{index_def['name']}"
            fields = ", ".join([f"[{field}]" for field in index_def["fields"]])
            sql_indexes.append(
                f"CREATE INDEX IF NOT EXISTS [{index_name}] ON [{self.table_name}] ({fields})"
            )
        return sql_indexes
