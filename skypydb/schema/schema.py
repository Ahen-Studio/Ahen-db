"""
Schema system for Skypydb.
"""

from typing import Any, Dict, List, Optional
from .values import Validator, v


# main class to define a table with columns and indexes
class TableDefinition:
    """
    Definition of a table with columns and indexes.
    """

    def __init__(
        self,
        columns: Dict[str, Validator],
        table_name: Optional[str] = None
    ):
        """
        Initialize table definition.

        Args:
            columns: Dictionary mapping column names to validators
            table_name: Optional table name (can be used later)
        """

        self.columns = columns
        self.indexes: List[Dict[str, Any]] = []
        self.table_name = table_name


    # add an index to the table definition
    def index(
        self,
        name: str,
        fields: List[str]
    ) -> "TableDefinition":
        """
        Add an index to the table definition.

        Args:
            name: Name of the index
            fields: List of column names to index

        Returns:
            Self for method chaining
        """

        # Validate that fields exist in columns
        for field in fields:
            if field not in self.columns:
                raise ValueError(
                    f"Cannot create index '{name}' on non-existent field '{field}'. "
                    f"Available fields: {list(self.columns.keys())}"
                )

        self.indexes.append({
            "name": name,
            "fields": fields
        })
        return self


    # validate a row of data against a table definition
    def validate_row(self, row_data: Dict[str, Any]) -> None:
        """
        Validate a row of data against this table definition.

        Args:
            row_data: Dictionary of column names to values

        Raises:
            ValueError: If validation fails or required columns are missing
        """

        # Check all required columns are present
        for col_name, validator in self.columns.items():
            if col_name not in row_data:
                # Check if the column is optional or required
                is_optional = getattr(validator, 'optional', False)
                if is_optional:
                    continue
                else:
                    raise ValueError(
                        f"Missing required column: '{col_name}'"
                    )

            value = row_data[col_name]
            if not validator.validate(value):
                raise ValueError(
                    f"Invalid value for column '{col_name}': "
                    f"expected {validator}, got {type(value).__name__}"
                )


    # retrieve the SQL column definitions for a table
    def get_sql_columns(self) -> List[str]:
        """
        Get SQL column definitions for this table.

        Returns:
            List of SQL column definitions
        """

        sql_columns = [
            "id TEXT PRIMARY KEY",
            "created_at TEXT NOT NULL"
        ]

        for col_name, validator in self.columns.items():
            base_validator = getattr(validator, "validator", validator)
            if col_name in ["id", "created_at"]:
                continue

            # Map validators to SQL types
            sql_type = "TEXT"  # Default for strings and other types

            if isinstance(base_validator, v.int64().__class__):
                sql_type = "INTEGER"
            elif isinstance(base_validator, v.float64().__class__):
                sql_type = "REAL"
            elif isinstance(base_validator, v.boolean().__class__):
                sql_type = "INTEGER"
            else:
                sql_type = "TEXT"

            sql_columns.append(f"[{col_name}] {sql_type}")

        return sql_columns


    # retrieve the SQL index definitions for a table
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


# main class to create table from a schema
class Schema:
    """
    Schema containing multiple table definitions.
    """

    def __init__(self, tables: Dict[str, TableDefinition]):
        """
        Initialize schema.

        Args:
            tables: Dictionary mapping table names to table definitions
        """

        self.tables = tables


    # retrieve a table definition by name
    def get_table_definition(self, table_name: str) -> Optional[TableDefinition]:
        """
        Get a table definition by name.

        Args:
            table_name: Name of the table

        Returns:
            TableDefinition if found, None otherwise
        """

        return self.tables.get(table_name)


    # retrieve all table names
    def get_all_table_names(self) -> List[str]:
        """
        Get all table names in the schema.

        Returns:
            List of table names
        """
        return list(self.tables.keys())


# 
def defineTable(
    columns: Dict[str, Validator],
) -> TableDefinition:
    """
    Define a table with its columns and types.

    Args:
        columns: Dictionary mapping column names to validators

    Returns:
        TableDefinition that can be configured with indexes

    Example:
        users = defineTable({
            "name": v.string(),
            "email": v.string(),
            "age": v.int64(),
            "active": v.boolean(),
            "bio": v.optional(v.string())
        })
        .index("by_email", ["email"])
        .index("by_age", ["age"])
    """
    return TableDefinition(columns)


# define a schema with multiple tables
def defineSchema(
    tables: Dict[str, TableDefinition],
) -> Schema:
    """
    Define a schema with multiple tables.

    Args:
        tables: Dictionary mapping table names to table definitions

    Returns:
        Schema object containing all tables

    Example:
        schema = defineSchema({
            "users": defineTable({
                "name": v.string(),
                "email": v.string()
            })
            .index("by_name", ["name"]),
            
            "posts": defineTable({
                "title": v.string(),
                "content": v.string()
            })
            .index("by_title", ["title"])
        })
    """

    # Set table names in definitions
    for table_name, table_def in tables.items():
        table_def.table_name = table_name

    return Schema(tables)
