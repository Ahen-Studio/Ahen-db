"""
Database backend using SQLite.
"""

import sqlite3
import uuid
import json
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, List, Optional

from ..errors import TableAlreadyExistsError, TableNotFoundError
from ..security.encryption import EncryptionManager
from ..security.validation import InputValidator, ValidationError
from ..schema.schema import TableDefinition


class Database:
    """
    Manages SQLite database connections and operations.
    
    All tables must be created via the schema system using defineSchema/defineTable.
    """

    def __init__(
        self,
        path: str,
        encryption_key: Optional[str] = None,
        salt: Optional[bytes] = None,
        encrypted_fields: Optional[list] = None,
    ):
        """
        Initialize SQLite database.

        Args:
            path: Path to SQLite database file
            encryption_key: Optional encryption key for data encryption
            salt: Optional salt for encryption key derivation
            encrypted_fields: Optional list of fields to encrypt
        """
        self.path = path
        self.encryption_key = encryption_key
        self.salt = salt
        self.encrypted_fields = encrypted_fields if encrypted_fields is not None else []

        # Initialize encryption manager
        self.encryption = EncryptionManager(encryption_key=encryption_key, salt=salt)

        # Create data directory if it doesn't exist
        Path(path).parent.mkdir(parents=True, exist_ok=True)

        # Connect to SQLite database
        self.conn = sqlite3.connect(path, check_same_thread=False)
        self.conn.row_factory = sqlite3.Row

        # Ensure config table exists
        self._ensure_config_table()

    def table_exists(
        self,
        table_name: str,
    ) -> bool:
        """
        Check if a table exists.
        """
        
        # Validate table name
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

    def delete_table(
        self,
        table_name: str,
    ) -> None:
        """
        Delete a table.

        Args:
            table_name: Name of the table to delete

        Raises:
            TableNotFoundError: If table doesn't exist
            ValidationError: If table name is invalid
        """
        
        # Validate table name
        table_name = InputValidator.validate_table_name(table_name)

        if not self.table_exists(table_name):
            raise TableNotFoundError(f"Table '{table_name}' not found")

        cursor = self.conn.cursor()

        cursor.execute(f"DROP TABLE [{table_name}]")
        self.conn.commit()

    def get_table_columns(
        self,
        table_name: str,
    ) -> List[str]:
        """
        Get list of column names for a table.
        """
        
        # Validate table name
        table_name = InputValidator.validate_table_name(table_name)

        if not self.table_exists(table_name):
            raise TableNotFoundError(f"Table '{table_name}' not found")

        cursor = self.conn.cursor()

        cursor.execute(f"PRAGMA table_info([{table_name}])")
        return [row[1] for row in cursor.fetchall()]

    def add_columns_if_needed(
        self,
        table_name: str,
        columns: List[str],
    ) -> None:
        """
        Add columns to a table if they don't exist.

        Args:
            table_name: Name of the table
            columns: List of column names to add
        """
        
        # Validate table name
        table_name = InputValidator.validate_table_name(table_name)

        existing_columns = set(self.get_table_columns(table_name))
        
        cursor = self.conn.cursor()

        for column in columns:
            # Validate column name
            validated_column = InputValidator.validate_column_name(column)
            if validated_column not in existing_columns and validated_column not in ("id", "created_at"):
                cursor.execute(f"ALTER TABLE [{table_name}] ADD COLUMN [{validated_column}] TEXT")

        self.conn.commit()

    def insert_data(
        self,
        table_name: str,
        data: Dict[str, Any],
        generate_id: bool = True,
    ) -> str:
        """
        Insert data into a table.

        Args:
            table_name: Name of the table
            data: Dictionary of column names and values
            generate_id: Whether to generate UUID automatically

        Returns:
            The ID of the inserted row
            
        Raises:
            ValidationError: If input data is invalid
        """
        
        # Validate table name
        table_name = InputValidator.validate_table_name(table_name)
        
        # Validate data dictionary
        data = InputValidator.validate_data_dict(data)

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

        # Encrypt sensitive data before storing
        encrypted_data = self._encrypt_data(data)

        # Build INSERT query
        columns = list(encrypted_data.keys())
        placeholders = ", ".join(["?" for _ in columns])
        column_names = ", ".join(columns)

        cursor = self.conn.cursor()

        cursor.execute(
            f"INSERT INTO [{table_name}] ({column_names}) VALUES ({placeholders})",
            [str(encrypted_data[col]) for col in columns],
        )
        self.conn.commit()

        return data["id"]

    def search(
        self,
        table_name: str,
        index: Optional[str] = None,
        **filters,
    ) -> List[Dict[str, Any]]:
        """
        Search for data in a table.

        Args:
            table_name: Name of the table
            index: Value to search for in any column (searches all columns if column not specified)
            **filters: Additional filters as keyword arguments (column name = value)

        Returns:
            List of dictionaries containing matching rows
            
        Raises:
            ValidationError: If input parameters are invalid
        """
        
        # Validate table name
        table_name = InputValidator.validate_table_name(table_name)
        
        # Validate filters
        if filters:
            filters = InputValidator.validate_filter_dict(filters)
        
        # Sanitize index value
        if index is not None:
            index = InputValidator.sanitize_string(str(index))

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

        # Convert rows to dictionaries and decrypt sensitive data
        results = []
        for row in cursor.fetchall():
            row_dict = dict(row)
            decrypted_row = self._decrypt_data(row_dict)
            results.append(decrypted_row)

        return results

    def get_all_tables(
        self,
    ) -> List[str]:
        """
        Get list of all table names.
        """

        cursor = self.conn.cursor()

        cursor.execute("SELECT name FROM sqlite_master WHERE type='table'")
        return [row[0] for row in cursor.fetchall()]

    def get_all_data(
        self,
        table_name: str,
    ) -> List[Dict[str, Any]]:
        """
        Get all data from a table.
        """
        
        # Validate table name
        table_name = InputValidator.validate_table_name(table_name)

        if not self.table_exists(table_name):
            raise TableNotFoundError(f"Table '{table_name}' not found")

        cursor = self.conn.cursor()

        cursor.execute(f"SELECT * FROM [{table_name}]")

        results = []
        for row in cursor.fetchall():
            row_dict = dict(row)
            decrypted_row = self._decrypt_data(row_dict)
            results.append(decrypted_row)

        return results

    def delete_data(
        self,
        table_name: str,
        **filters,
    ) -> int:
        """
        Delete data from a table based on filters.

        Args:
            table_name: Name of the table
            **filters: Filters as keyword arguments (column name = value)

        Returns:
            Number of rows deleted

        Example:
            db.delete_data(
                table_name="my_table",
                id="123"
            )
            db.delete_data(
                table_name="my_table",
                user_id="user123",
                title="document"
            )
            
        Raises:
            ValidationError: If input parameters are invalid
        """
        
        # Validate table name
        table_name = InputValidator.validate_table_name(table_name)
        
        # Validate filters
        if filters:
            filters = InputValidator.validate_filter_dict(filters)

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

    def _ensure_config_table(
        self,
    ) -> None:
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

    def save_table_config(
        self,
        table_name: str,
        config: Dict[str, Any],
    ) -> None:
        """
        Save a table's configuration to the system table.

        Args:
            table_name: Name of the table
            config: Configuration dictionary for the table
        """
        
        # Validate table name
        table_name = InputValidator.validate_table_name(table_name)

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

    def _normalize_config(
        self,
        config: Dict[str, Any],
    ) -> Dict[str, str]:
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

    def get_table_config(
        self,
        table_name: str,
    ) -> Optional[Dict[str, Any]]:
        """
        Retrieve a table's configuration from the system table.

        Args:
            table_name: Name of the table

        Returns:
            Configuration dictionary or None if not found
        """
        
        # Validate table name
        table_name = InputValidator.validate_table_name(table_name)

        cursor = self.conn.cursor()

        cursor.execute(
            "SELECT config FROM _skypy_config WHERE table_name = ?", (table_name,)
        )
        row = cursor.fetchone()

        if row:
            return json.loads(row[0])
        return None



    def create_table_from_schema(
        self,
        table_name: str,
        table_def: TableDefinition,
    ) -> None:
        """
        Create a table based on a TableDefinition from the schema system.

        Args:
            table_name: Name of the table to create
            table_def: TableDefinition containing columns and indexes

        Raises:
            TableAlreadyExistsError: If table already exists
            ValidationError: If table definition is invalid

        Example:
            table_def = defineTable({
                "name": v.string(),
                "email": v.string()
            })
            .index("by_email", ["email"])

            db.create_table_from_schema("users", table_def)
        """

        # Validate table name
        table_name = InputValidator.validate_table_name(table_name)

        # Validate column names
        for col_name in table_def.columns.keys():
            InputValidator.validate_column_name(col_name)

        if self.table_exists(table_name):
            raise TableAlreadyExistsError(f"Table '{table_name}' already exists")

        cursor = self.conn.cursor()

        # Get SQL column definitions from table definition
        sql_columns = table_def.get_sql_columns()
        columns_sql = ", ".join(sql_columns)

        # Create table
        cursor.execute(
            f"""
            CREATE TABLE [{table_name}] (
                {columns_sql}
            )
            """
        )

        # Create indexes
        for index_sql in table_def.get_sql_indexes():
            cursor.execute(index_sql)

        # Save table definition as configuration
        config = self._table_def_to_config(table_def)
        self.save_table_config(table_name, config)
        self.conn.commit()

    def _table_def_to_config(
        self,
        table_def: TableDefinition,
    ) -> Dict[str, Any]:
        """
        Convert a TableDefinition to a config dictionary for storage.

        Args:
            table_def: TableDefinition to convert

        Returns:
            Configuration dictionary
        """
        config = {}

        # Convert validators to type strings
        for col_name, validator in table_def.columns.items():
            validator_repr = repr(validator)
            
            # Map validator repr to config type
            # Check for optional first before checking inner types
            if "v.optional(" in validator_repr:
                # Extract the base type from optional
                if "v.string()" in validator_repr:
                    config[col_name] = {"type": "str", "optional": True}
                elif "v.int64()" in validator_repr:
                    config[col_name] = {"type": "int", "optional": True}
                elif "v.float64()" in validator_repr:
                    config[col_name] = {"type": "float", "optional": True}
                elif "v.boolean()" in validator_repr:
                    config[col_name] = {"type": "bool", "optional": True}
                else:
                    config[col_name] = {"type": "str", "optional": True}
            elif "v.string()" in validator_repr:
                config[col_name] = "str"
            elif "v.int64()" in validator_repr:
                config[col_name] = "int"
            elif "v.float64()" in validator_repr:
                config[col_name] = "float"
            elif "v.boolean()" in validator_repr:
                config[col_name] = "bool"
            else:
                config[col_name] = "str"  # Default

        # Add index information
        if table_def.indexes:
            config["_indexes"] = [
                {"name": idx["name"], "fields": idx["fields"]}
                for idx in table_def.indexes
            ]

        return config

    def validate_data_with_config(
        self,
        table_name: str,
        data: Dict[str, Any],
    ) -> Dict[str, Any]:
        """
        Validate data against the table's configuration.
        Converts values to the correct type based on configuration.

        Args:
            table_name: Name of the table
            data: Data dictionary to validate

        Returns:
            Validated data dictionary with converted values

        Raises:
            ValueError: If data validation fails
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

    def delete_table_config(
        self,
        table_name: str,
    ) -> None:
        """
        Delete a table's configuration from the system table.

        Args:
            table_name: Name of the table
            
        Raises:
            ValidationError: If input parameters are invalid
        """
        
        # Validate table name
        table_name = InputValidator.validate_table_name(table_name)

        cursor = self.conn.cursor()

        cursor.execute("DELETE FROM _skypy_config WHERE table_name = ?", (table_name,))
        self.conn.commit()

    def _encrypt_data(
        self,
        data: Dict[str, Any],
    ) -> Dict[str, Any]:
        """
        Encrypt sensitive fields in data dictionary.

        Args:
            data: Dictionary containing data to encrypt

        Returns:
            Dictionary with encrypted fields
        """
        
        if not self.encryption.enabled:
            return data

        # Determine which fields to encrypt
        fields_to_encrypt = [
            key for key in data.keys() 
            if key in self.encrypted_fields
        ]

        return self.encryption.encrypt_dict(data, fields_to_encrypt)

    def _decrypt_data(
        self,
        data: Dict[str, Any],
    ) -> Dict[str, Any]:
        """
        Decrypt sensitive fields in data dictionary.

        Args:
            data: Dictionary containing encrypted data

        Returns:
            Dictionary with decrypted fields
        """
        
        if not self.encryption.enabled:
            return data

        # Determine which fields to decrypt
        fields_to_decrypt = [
            key for key in data.keys() 
            if key in self.encrypted_fields
        ]

        return self.encryption.decrypt_dict(data, fields_to_decrypt)

    def close(
        self,
    ) -> None:
        """
        Close database connection.
        """

        if self.conn:
            self.conn.close()
