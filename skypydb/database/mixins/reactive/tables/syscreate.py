"""
Module containing the SysCreate class, which is used to create tables in the database.
"""

import sqlite3
from typing import Optional
from skypydb.errors import TableAlreadyExistsError
from skypydb.security.validation import InputValidator
from skypydb.schema.schema import TableDefinition
from skypydb.database.mixins.reactive.tables.audit import AuditTable
from skypydb.database.mixins.reactive.utils import Utils

class SysCreate:
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

    def create_table(
        self,
        table_name: str,
        table_def: TableDefinition
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

            database.create_table("users", table_def)
        """

        # validate table name
        table_name = InputValidator.validate_table_name(table_name)

        # validate column names
        for col_name in table_def.columns.keys():
            InputValidator.validate_column_name(col_name)
        if self.audit.table_exists(table_name):
            raise TableAlreadyExistsError(f"Table '{table_name}' already exists")

        cursor = self.conn.cursor()

        # get SQL column definitions from table definition
        sql_columns = table_def.get_sql_columns()
        columns_sql = ", ".join(sql_columns)

        # create table
        cursor.execute(
            f"""
            CREATE TABLE [{table_name}] (
                {columns_sql}
            )
            """
        )
        # create indexes
        for index_sql in table_def.get_sql_indexes():
            cursor.execute(index_sql)

        # save table definition as configuration
        config = self.utils.table_def_to_config(table_def)
        self.utils.save_table_config(table_name, config)
        self.conn.commit()
