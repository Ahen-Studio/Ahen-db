"""
Module containing the SysDelete class, which is used to delete tables in the database.
"""

import sqlite3
from typing import Optional
from skypydb.security.validation import InputValidator
from skypydb.errors import TableNotFoundError
from skypydb.database.mixins.reactive.tables.audit import AuditTable
from skypydb.database.mixins.reactive.utils import Utils

class SysDelete:
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

        self.audit = AuditTable(conn=self.conn)
        self.utils = Utils(conn=self.conn)

    def delete_table(
        self,
        table_name: str
    ) -> None:
        """
        Delete a table.

        Args:
            table_name: Name of the table to delete

        Raises:
            TableNotFoundError: If table doesn't exist
            ValidationError: If table name is invalid
        """

        # validate table name
        table_name = InputValidator.validate_table_name(table_name)
        if not self.audit.table_exists(table_name):
            raise TableNotFoundError(f"Table '{table_name}' not found")

        cursor = self.conn.cursor()

        cursor.execute(f"DROP TABLE [{table_name}]")

        self.utils.delete_table_config(table_name)

        self.conn.commit()
