"""
Module containing the RSysSearch class, which is used to search tables in the database.
"""

import sqlite3
from typing import (
    Any,
    Dict,
    List,
    Optional
)
from skypydb.errors import TableNotFoundError
from skypydb.security.validation import InputValidator
from skypydb.errors import ValidationError
from skypydb.database.mixins.reactive.tables.audit import AuditTable
from skypydb.database.mixins.reactive.tables.sysget import SysGet
from skypydb.database.mixins.reactive.encryption import Encryption

class RSysSearch:
    def __init__(
        self,
        path: Optional[str] = None,
        conn: Optional[sqlite3.Connection] = None,
        encryption: Optional[Encryption] = None
    ):
        if conn is not None:
            self.conn = conn
        elif path is not None:
            self.conn = sqlite3.connect(path, check_same_thread=False)
            self.conn.row_factory = sqlite3.Row
        else:
            raise ValueError("Either path or conn must be provided")

        self.audit = AuditTable(conn=self.conn)
        self.sysget = SysGet(conn=self.conn, encryption=encryption)
        self.encryption = encryption

    def search(
        self,
        table_name: str,
        index: Optional[str] = None,
        **filters
    ) -> List[Dict[str, Any]]:
        """
        Search a table for rows that match an optional free text index value and/or column filters.

        Parameters:
            table_name (str): Name of the target table.
            index (Optional[str]): Value to search across all non-standard columns; ignored if None.
            **filters: Column-value pairs to filter results. If a value is a list, it is used with an IN clause (empty lists are invalid).

        Returns:
            List[Dict[str, Any]]: Matching rows as dictionaries with sensitive fields decrypted.

        Raises:
            ValidationError: If table name, index, or filters fail validation, or an empty list is provided for a filter.
            TableNotFoundError: If the specified table does not exist.
        """

        # validate table name
        table_name = InputValidator.validate_table_name(table_name)

        # validate filters
        if filters:
            filters = InputValidator.validate_filter_dict(filters)
        # sanitize index value
        if index is not None:
            index = InputValidator.sanitize_string(str(index))
        if not self.audit.table_exists(table_name):
            raise TableNotFoundError(f"Table '{table_name}' not found")

        conditions = []
        params = []

        # add index condition if provided
        # index searches across all non-standard columns (OR condition)
        if index is not None:
            columns = self.sysget.get_table_columns_names(table_name)
            non_standard_columns = [
                col for col in columns if col not in ("id", "created_at")
            ]
            if non_standard_columns:
                # search index value in any of the non-standard columns
                index_conditions = []
                for col in non_standard_columns:
                    index_conditions.append(f"[{col}] = ?")
                    params.append(str(index))
                conditions.append(f"({' OR '.join(index_conditions)})")
        # add additional filters (AND conditions)
        for column, value in filters.items():
            # handle list values, use IN clause
            if isinstance(value, list):
                if not value:
                    raise ValidationError(f"Empty list provided for filter '{column}'")
                placeholders = ", ".join(["?" for _ in value])
                conditions.append(f"[{column}] IN ({placeholders})")
                params.extend([str(v) for v in value])
            else:
                conditions.append(f"[{column}] = ?")
                params.append(str(value))

        # build query
        where_clause = " AND ".join(conditions) if conditions else "1=1"
        query = f"SELECT * FROM [{table_name}] WHERE {where_clause}"

        cursor = self.conn.cursor()

        cursor.execute(query, params)

        # convert rows to dictionaries and decrypt sensitive data
        results = []
        for row in cursor.fetchall():
            row_dict = dict(row)
            if self.encryption:
                decrypted_row = self.encryption.decrypt_data(row_dict)
                results.append(decrypted_row)
            else:
                results.append(row_dict)
        return results
