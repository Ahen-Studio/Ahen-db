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

    def get_or_create_table(self) -> dict:
        """
        Get existing tables from schema or create them if missing.

        Returns:
            Dictionary mapping table names to Table instances

        Example:
            tables = client.get_or_create_table()
            users_table = tables["users"]
        """

        schema = self._load_schema()

        created_tables = {}
        table_names = schema.get_all_table_names()

        for table_name in table_names:
            table_def = schema.get_table_definition(table_name)
            if table_def is None:
                continue
            if self.db.table_exists(table_name):
                created_tables[table_name] = Table(self.db, table_name)
                continue
            self.db.create_table(table_name, table_def)
            created_tables[table_name] = Table(self.db, table_name)
        return created_tables