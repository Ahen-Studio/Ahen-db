"""
Database backend using SQLite.
"""

import sqlite3
import uuid
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, List, Optional

from ..errors import TableAlreadyExistsError, TableNotFoundError


class Database:
    """
    Manages SQLite database connections and operations.
    """

    def __init__(self, path: str):
        """
        Initialize database connection.

        Args:
            path: Path to SQLite database file
        """

        self.path = Path(path)
        self.path.parent.mkdir(parents=True, exist_ok=True)
        self.conn = sqlite3.connect(str(self.path), check_same_thread=False)
        self.conn.row_factory = sqlite3.Row  # Enable dict-like access
        self._ensure_config_table()

    def create_table(self, table_name: str) -> None:
        """
        Create a new table with id and created_at columns.

        Args:
            table_name: Name of the table to create

        Raises:
            TableAlreadyExistsError: If table already exists
        """

        # Check if table exists
        cursor = self.conn.cursor()

        cursor.execute(
            "SELECT name FROM sqlite_master WHERE type='table' AND name=?",
            (table_name,),
        )
        if cursor.fetchone():
            raise TableAlreadyExistsError(f"Table '{table_name}' already exists")

        # Create table with id and created_at columns
        cursor.execute(
            f"""
            CREATE TABLE IF NOT EXISTS [{table_name}] (
                id TEXT PRIMARY KEY,
                created_at TEXT NOT NULL
            )
            """
        )
        self.conn.commit()

    def table_exists(self, table_name: str) -> bool:
        """
        Check if a table exists.
        """

        cursor = self.conn.cursor()

        cursor.execute(
            "SELECT name FROM sqlite_master WHERE type='table' AND name=?",
            (table_name,),
        )
        return cursor.fetchone() is not None

    def delete_table(self, table_name: str) -> None:
        """
        Delete a table.

        Args:
            table_name: Name of the table to delete

        Raises:
            TableNotFoundError: If table doesn't exist
        """

        if not self.table_exists(table_name):
            raise TableNotFoundError(f"Table '{table_name}' not found")

        cursor = self.conn.cursor()

        cursor.execute(f"DROP TABLE [{table_name}]")
        self.conn.commit()

    def get_table_columns(self, table_name: str) -> List[str]:
        """
        Get list of column names for a table.
        """

        if not self.table_exists(table_name):
            raise TableNotFoundError(f"Table '{table_name}' not found")

        cursor = self.conn.cursor()

        cursor.execute(f"PRAGMA table_info([{table_name}])")
        return [row[1] for row in cursor.fetchall()]

    def add_columns_if_needed(self, table_name: str, columns: List[str]) -> None:
        """
        Ensure the given columns exist on a table by adding any that are missing as TEXT columns; ignores the reserved columns "id" and "created_at". Changes are committed to the database.
        
        Parameters:
            table_name (str): Name of the target table.
            columns (List[str]): Column names to ensure exist on the table.
        """

        existing_columns = set(self.get_table_columns(table_name))
        
        cursor = self.conn.cursor()

        for column in columns:
            if column not in existing_columns and column not in ("id", "created_at"):
                cursor.execute(f"ALTER TABLE [{table_name}] ADD COLUMN [{column}] TEXT")

        self.conn.commit()

    def insert_data(
        self, 
        table_name: str, 
        data: Dict[str, Any], 
        generate_id: bool = True
    ) -> str:
        """
        Insert a row into the specified table.
        
        If generate_id is True, a UUID is assigned to the `id` field. Ensures a `created_at` ISO timestamp is present and adds any missing columns (excluding `id` and `created_at`) before inserting.
        
        Parameters:
            table_name (str): Name of the table to insert into.
            data (Dict[str, Any]): Mapping of column names to values; may be modified to include generated `id` and `created_at`.
            generate_id (bool): Whether to generate and set an `id` for the row.
        
        Returns:
            str: The `id` of the inserted row.
        
        Raises:
            TableNotFoundError: If the specified table does not exist.
        """

        if not self.table_exists(table_name):
            raise TableNotFoundError(f"Table '{table_name}' not found")

        # Generate ID if needed
        if generate_id:
            data["id"] = str(uuid.uuid4())

        # Add created_at timestamp
        if "created_at" not in data:
            data["created_at"] = datetime.now().isoformat()

        # Ensure columns exist
        columns_to_add = [col for col in data.keys() if col not in ("id", "created_at")]
        if columns_to_add:
            self.add_columns_if_needed(table_name, columns_to_add)

        # Build INSERT query
        columns = list(data.keys())
        placeholders = ", ".join(["?" for _ in columns])
        column_names = ", ".join(columns)

        cursor = self.conn.cursor()

        cursor.execute(
            f"INSERT INTO [{table_name}] ({column_names}) VALUES ({placeholders})",
            [str(data[col]) for col in columns],
        )
        self.conn.commit()

        return data["id"]

    def search(
        self, table_name: str, 
        index: Optional[str] = None, 
        **filters
    ) -> List[Dict[str, Any]]:
        """
        Searches rows in a table using an optional index value and additional column filters.
        
        If `index` is provided it is matched against all non-standard columns (excluding "id" and "created_at"). Additional keyword filters are combined with logical AND; filter values that are lists are treated as IN clauses.
        
        Parameters:
            table_name (str): Name of the table to search.
            index (Optional[str]): Value to match across all non-standard columns when provided.
            **filters: Column-specific filters as keyword arguments (column=value). List values are treated as membership (IN) filters.
        
        Returns:
            List[Dict[str, Any]]: A list of rows as dictionaries keyed by column name.
        
        Raises:
            TableNotFoundError: If the specified table does not exist.
        """

        if not self.table_exists(table_name):
            raise TableNotFoundError(f"Table '{table_name}' not found")

        conditions = []
        params = []

        # Add index condition if provided
        # Index searches across all non-standard columns (OR condition)
        if index is not None:
            columns = self.get_table_columns(table_name)
            non_standard_columns = [
                col for col in columns if col not in ("id", "created_at")
            ]

            if non_standard_columns:
                # Search index value in any of the non-standard columns
                index_conditions = []
                for col in non_standard_columns:
                    index_conditions.append(f"[{col}] = ?")
                    params.append(str(index))
                conditions.append(f"({' OR '.join(index_conditions)})")

        # Add additional filters (AND conditions)
        for column, value in filters.items():
            if column not in ("id", "created_at"):
                # Handle list values - use IN clause
                if isinstance(value, list) and len(value) > 0:
                    placeholders = ", ".join(["?" for _ in value])
                    conditions.append(f"[{column}] IN ({placeholders})")
                    params.extend([str(v) for v in value])
                else:
                    conditions.append(f"[{column}] = ?")
                    params.append(str(value))

        # Build query
        where_clause = " AND ".join(conditions) if conditions else "1=1"
        query = f"SELECT * FROM [{table_name}] WHERE {where_clause}"

        cursor = self.conn.cursor()
        
        cursor.execute(query, params)

        # Convert rows to dictionaries
        results = []
        for row in cursor.fetchall():
            results.append(dict(row))

        return results

    def get_all_tables(self) -> List[str]:
        """
        Get list of all table names.
        """

        cursor = self.conn.cursor()

        cursor.execute("SELECT name FROM sqlite_master WHERE type='table'")
        return [row[0] for row in cursor.fetchall()]

    def get_all_data(self, table_name: str) -> List[Dict[str, Any]]:
        """
        Retrieve all rows from the specified table as a list of dictionaries.
        
        Returns:
            List[Dict[str, Any]]: A list where each item is a row represented as a mapping from column names to values.
        
        Raises:
            TableNotFoundError: If the specified table does not exist.
        """

        if not self.table_exists(table_name):
            raise TableNotFoundError(f"Table '{table_name}' not found")

        cursor = self.conn.cursor()

        cursor.execute(f"SELECT * FROM [{table_name}]")

        results = []
        for row in cursor.fetchall():
            results.append(dict(row))

        return results

    def delete_data(self, table_name: str, **filters) -> int:
        """
        Delete rows from a table that match the provided column filters.
        
        Filters supplied as keyword arguments are applied as equality checks; if a filter value is a list, it is used with an SQL IN clause. Column names are taken from the keyword names and values are cast to strings before execution.
        
        Parameters:
            table_name (str): Name of the table to delete rows from.
            **filters: Column filters where a scalar value means equality and a list value produces an IN clause.
        
        Returns:
            int: Number of rows deleted.
        
        Raises:
            TableNotFoundError: If the specified table does not exist.
            ValueError: If no filters are provided (to prevent deleting all rows).
        """

        if not self.table_exists(table_name):
            raise TableNotFoundError(f"Table '{table_name}' not found")

        if not filters:
            # Safety check - don't allow deleting all rows without explicit filters
            raise ValueError("Cannot delete without filters. Use filters to specify which rows to delete.")

        conditions = []
        params = []

        # Build WHERE clause from filters
        for column, value in filters.items():
            # Handle list values - use IN clause
            if isinstance(value, list) and len(value) > 0:
                placeholders = ", ".join(["?" for _ in value])
                conditions.append(f"[{column}] IN ({placeholders})")
                params.extend([str(v) for v in value])
            else:
                conditions.append(f"[{column}] = ?")
                params.append(str(value))

        # Build DELETE query
        where_clause = " AND ".join(conditions)
        query = f"DELETE FROM [{table_name}] WHERE {where_clause}"

        cursor = self.conn.cursor()

        cursor.execute(query, params)
        self.conn.commit()

        return cursor.rowcount

    def _ensure_config_table(self) -> None:
        """
        Ensure the system configuration table exists.
        
        Creates the `_skypy_config` table if absent with columns:
        `table_name` (TEXT primary key), `config` (TEXT NOT NULL), and `created_at` (TEXT NOT NULL).
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

    def save_table_config(self, table_name: str, config: Dict[str, Any]) -> None:
        """
        Save a table's configuration to the system table.

        Args:
            table_name: Name of the table
            config: Configuration dictionary for the table
        """

        import json

        # Normalize config to ensure types are strings for JSON serialization
        normalized_config = self._normalize_config(config)

        cursor = self.conn.cursor()

        cursor.execute(
            """
            INSERT OR REPLACE INTO _skypy_config (table_name, config, created_at)
            VALUES (?, ?, ?)
            """,
            (table_name, json.dumps(normalized_config), datetime.now().isoformat()),
        )
        self.conn.commit()

    def _normalize_config(self, config: Dict[str, Any]) -> Dict[str, str]:
        """
        Normalize configuration to ensure all types are strings for JSON serialization.

        Args:
            config: Configuration dictionary with columns and types

        Returns:
            Normalized configuration with string types
        """

        normalized = {}
        for col_name, col_type in config.items():
            # Convert type objects to their string representation
            if col_type is str or isinstance(col_type, type) and col_type is str:
                normalized[col_name] = "str"
            elif col_type is int or isinstance(col_type, type) and col_type is int:
                normalized[col_name] = "int"
            elif col_type is float or isinstance(col_type, type) and col_type is float:
                normalized[col_name] = "float"
            elif col_type is bool or isinstance(col_type, type) and col_type is bool:
                normalized[col_name] = "bool"
            else:
                # Keep as is if it's already a string or special value
                normalized[col_name] = str(col_type)

        return normalized

    def get_table_config(self, table_name: str) -> Optional[Dict[str, Any]]:
        """
        Retrieve a table's configuration from the system table.

        Args:
            table_name: Name of the table

        Returns:
            Configuration dictionary or None if not found
        """

        import json

        cursor = self.conn.cursor()

        cursor.execute(
            "SELECT config FROM _skypy_config WHERE table_name = ?", (table_name,)
        )
        row = cursor.fetchone()

        if row:
            return json.loads(row[0])
        return None

    def create_table_from_config(self, table_name: str, config: Dict[str, Any]) -> None:
        """
        Create a new table from a configuration and store that configuration in the system config table.
        
        Parameters:
            table_name (str): Name of the table to create.
            config (Dict[str, Any]): Mapping of column names to types (e.g., "str", int, "auto"). The key "id" is reserved and ignored in the configuration; the created table will always include an `id` primary key and a non-null `created_at` column.
        
        Raises:
            TableAlreadyExistsError: If a table with the given name already exists.
        """

        if self.table_exists(table_name):
            raise TableAlreadyExistsError(f"Table '{table_name}' already exists")

        cursor = self.conn.cursor()

        # Build column definitions
        column_defs = []
        for col_name, col_type in config.items():
            if col_name == "id":
                continue  # ID is handled separately
            column_defs.append(f"[{col_name}] TEXT")

        # Create table with id, created_at, and configured columns
        columns_sql = ", ".join(
            ["id TEXT PRIMARY KEY", "created_at TEXT NOT NULL"] + column_defs
        )
        cursor.execute(
            f"""
            CREATE TABLE [{table_name}] (
                {columns_sql}
            )
            """
        )

        # Save configuration
        self.save_table_config(table_name, config)
        self.conn.commit()

    def validate_data_with_config(
        self,
        table_name: str,
        data: Dict[str, Any]
    ) -> Dict[str, Any]:
        """
        Validate and coerce fields in `data` according to the table's stored configuration.
        
        Parameters:
            table_name (str): Table whose configuration will be used for validation.
            data (Dict[str, Any]): Mapping of column names to values to validate and convert.
        
        Returns:
            Dict[str, Any]: A dictionary with the same keys as `data` (except keys with type `"auto"` or `"id"` are skipped) where values have been converted to the configured types.
        
        Raises:
            ValueError: If a value cannot be converted to the configured `int` or `float` type.
        """

        config = self.get_table_config(table_name)
        if not config:
            # No configuration, return data as-is
            return data

        validated_data = {}

        for key, value in data.items():
            if key in config:
                expected_type = config[key]

                # Skip "auto" type
                if expected_type == "auto" or expected_type == "id":
                    continue

                # Type conversion and validation
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
                    # Unknown type, store as string
                    validated_data[key] = str(value)
            else:
                # Column not in config, store as-is
                validated_data[key] = value

        return validated_data

    def delete_table_config(self, table_name: str) -> None:
        """
        Remove the saved configuration for a table from the internal system config table.
        
        Parameters:
            table_name (str): Name of the table whose configuration will be removed.
        """

        cursor = self.conn.cursor()

        cursor.execute("DELETE FROM _skypy_config WHERE table_name = ?", (table_name,))
        self.conn.commit()

    def close(self) -> None:
        """
        Close database connection.
        """

        if self.conn:
            self.conn.close()