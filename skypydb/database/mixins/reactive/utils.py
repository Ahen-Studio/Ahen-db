"""
Module containing the Utils class, which is used to do a bunch of things with tables in the database.
"""

import sqlite3
import json
from datetime import datetime
from typing import (
    Optional,
    Dict,
    Any
)
from skypydb.security.validation import InputValidator
from skypydb.schema.schema import TableDefinition

class Utils:
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

    def get_table_config(
        self,
        table_name: str
    ) -> Optional[Dict[str, Any]]:
        """
        Retrieve a table's configuration from the system table.

        Args:
            table_name: Name of the table

        Returns:
            Configuration dictionary or None if not found
        """

        # validate table name
        table_name = InputValidator.validate_table_name(table_name)

        cursor = self.conn.cursor()

        cursor.execute(
            "SELECT config FROM _skypy_config WHERE table_name = ?", (table_name,)
        )
        row = cursor.fetchone()
        if row:
            return json.loads(row[0])
        return None

    def save_table_config(
        self,
        table_name: str,
        config: Dict[str, Any]
    ) -> None:
        """
        Save a table's configuration to the system table.

        Args:
            table_name: Name of the table
            config: Configuration dictionary for the table
        """

        # validate table name
        table_name = InputValidator.validate_table_name(table_name)

        # normalize config to ensure types are strings for JSON serialization
        normalized_config = self.normalize_config(config)

        cursor = self.conn.cursor()

        cursor.execute(
            """
            INSERT OR REPLACE INTO _skypy_config (table_name, config, created_at)
            VALUES (?, ?, ?)
            """,
            (table_name, json.dumps(normalized_config), datetime.now().isoformat()),
        )
        self.conn.commit()


    def normalize_config(
        self,
        config: Dict[str, Any]
    ) -> Dict[str, Any]:
        """
        Normalize a table configuration so every column type is represented as a JSON-serializable value.

        For each entry in `config`:
        - If the value is a dict, returns a copy with its `"type"` field converted to one of the strings `"str"`, `"int"`, `"float"`, or `"bool"` when the type exactly matches the corresponding Python type; otherwise the type is converted to its string form. Other keys in the dict are preserved.
        - If the value is a list, it is preserved unchanged.
        - For any other value, the value is replaced by the normalized type string as described above.

        Parameters:
            config (Dict[str, Any]): Mapping of column names to type descriptors (type, dict, or list).

        Returns:
            Dict[str, Any]: Normalized configuration with JSON-serializable type representations.
        """

        def _normalize_type(t: Any) -> str:
            """
            Map a Python type object to a canonical string name used in stored table configurations.

            Parameters:
                t (Any): A type object or value; exact matches to the builtin types `str`, `int`, `float`, and `bool` produce their canonical names.

            Returns:
                A string: `'str'` if `t` is `str`, `'int'` if `t` is `int`, `'float'` if `t` is `float`, `'bool'` if `t` is `bool`, otherwise `str(t)`.
            """

            if t is str:
                return "str"
            if t is int:
                return "int"
            if t is float:
                return "float"
            if t is bool:
                return "bool"
            return str(t)

        normalized: Dict[str, Any] = {}
        for col_name, col_type in config.items():
            if isinstance(col_type, dict):
                normalized[col_name] = {
                    **col_type,
                    "type": _normalize_type(col_type.get("type", "str")),
                }
            elif isinstance(col_type, list):
                normalized[col_name] = col_type
            else:
                normalized[col_name] = _normalize_type(col_type)
        return normalized

    def table_def_to_config(
        self,
        table_def: TableDefinition
    ) -> Dict[str, Any]:
        """
        Convert a TableDefinition to a config dictionary for storage.

        Args:
            table_def: TableDefinition to convert

        Returns:
            Configuration dictionary
        """

        config = {}

        # convert validators to type strings
        for col_name, validator in table_def.columns.items():
            validator_repr = repr(validator)
            # map validator repr to config type
            # check for optional first before checking inner types
            if "v.optional(" in validator_repr:
                # extract the base type from optional
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
                config[col_name] = "str"  # default
        # add index information
        if table_def.indexes:
            config["_indexes"] = [
                {"name": idx["name"], "fields": idx["fields"]}
                for idx in table_def.indexes
            ]
        return config

    def delete_table_config(
        self,
        table_name: str
    ) -> None:
        """
        Delete a table's configuration from the system table.

        Args:
            table_name: Name of the table

        Raises:
            ValidationError: If input parameters are invalid
        """

        # validate table name
        table_name = InputValidator.validate_table_name(table_name)

        cursor = self.conn.cursor()

        cursor.execute("DELETE FROM _skypy_config WHERE table_name = ?", (table_name,))
        self.conn.commit()
