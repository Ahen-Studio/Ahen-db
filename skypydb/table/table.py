"""
Table API for managing data.
"""

from skypydb.database.reactive_db import ReactiveDatabase
from skypydb.errors import TableNotFoundError
from skypydb.table.mixins import (
    SysAdd,
    SysDelete,
    SysGet,
    SysSearch
)

class Table(
    SysAdd,
    SysDelete,
    SysGet,
    SysSearch
):
    """
    Represents a table in the database.
    """

    def __init__(
        self,
        db: ReactiveDatabase,
        table_name: str
    ):
        """
        Initialize table.

        Args:
            db: Database instance
            table_name: Name of the table
        """

        self.db = db
        self.table_name = table_name
        if not self.db.table_exists(table_name):
            raise TableNotFoundError(f"Table '{table_name}' not found")
