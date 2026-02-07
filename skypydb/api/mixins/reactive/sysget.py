"""
Module containing the SysGet class, which is used to get a table.
"""

from skypydb.errors import TableNotFoundError
from skypydb.table.table import Table

class SysGet:
    def get_table(
        self,
        table_name: str
    ) -> Table:
        """
        Get an existing table by name.

        Args:
            table_name: Name of the table

        Returns:
            Table instance

        Raises:
            TableNotFoundError: If table doesn't exist

        Example:
            tables = client.create_table()
            users_table = tables["users"]
            
            # Later, get the table again:
            users = client.get_table("users")
        """

        if not self.db.table_exists(table_name):
            raise TableNotFoundError(f"Table '{table_name}' not found")
        return Table(self.db, table_name)