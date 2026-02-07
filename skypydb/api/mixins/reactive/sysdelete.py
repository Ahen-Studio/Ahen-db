"""
Module containing the SysDelete class, which is used to delete a table.
"""

from skypydb.errors import TableNotFoundError

class SysDelete:
    def delete_table(
        self,
        table_name: str
    ) -> None:
        """
        Delete a table and its configuration.

        Args:
            table_name: Name of the table to delete

        Raises:
            TableNotFoundError: If table doesn't exist

        Example:
            client.delete_table("users")
        """

        if not self.db.table_exists(table_name):
            raise TableNotFoundError(f"Table '{table_name}' not found")

        self.db.delete_table(table_name)
        self.db.delete_table_config(table_name)