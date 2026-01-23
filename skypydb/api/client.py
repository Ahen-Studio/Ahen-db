"""
Client API for Skypydb.
"""

from typing import Any, Dict, Optional, Union

from ..db.database import Database
from ..errors import TableAlreadyExistsError, TableNotFoundError
from ..table.table import Table


class Client:
    """
    Main client for interacting with Skypydb.
    """

    def __init__(
        self,
        path: str,
        encryption_key: Optional[str] = None,
        salt: Optional[bytes] = None,
        encrypted_fields: Optional[list] = None,
    ):
        """
        Initialize Skypydb client.

        Args:
            path: Path to SQLite database file
            encryption_key: Optional encryption key for data encryption at rest.
                           If provided, sensitive data will be encrypted.
                           Generate a secure key with: EncryptionManager.generate_key()
            salt: Required, non-empty salt for PBKDF2 when encryption is enabled.
            encrypted_fields: Optional list of field names to encrypt.
                             If None and encryption is enabled, all fields except
                             'id' and 'created_at' will be encrypted.
                             
        Example:
            # Without encryption
            client = skypydb.Client(path="./data/skypy.db")
            
            # With encryption (all fields encrypted by default)
            from skypydb.security import EncryptionManager
            key = EncryptionManager.generate_key()
            client = skypydb.Client(
                path="./data/skypy.db",
                encryption_key=key
            )
            
            # With encryption (specific fields only)
            client = skypydb.Client(
                path="./data/skypy.db",
                encryption_key=key,
                encrypted_fields=["content", "email", "password"]
            )
        """

        self.path = path
        self.db = Database(path, encryption_key=encryption_key, salt=salt, encrypted_fields=encrypted_fields)

    def create_table(
        self,
        table_name: str,
    ) -> Table:
        """
        Create a new table.

        Args:
            table_name: Name of the table to create

        Returns:
            Table instance

        Raises:
            TableAlreadyExistsError: If table already exists
        """

        if self.db.table_exists(table_name):
            raise TableAlreadyExistsError(f"Table '{table_name}' already exists")

        self.db.create_table(table_name)
        return Table(self.db, table_name)

    def get_table(
        self,
        table_name: str,
    ) -> Table:
        """
        Get an existing table.

        Args:
            table_name: Name of the table

        Returns:
            Table instance

        Raises:
            TableNotFoundError: If table doesn't exist
        """

        if not self.db.table_exists(table_name):
            raise TableNotFoundError(f"Table '{table_name}' not found")
        return Table(self.db, table_name)

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
        """

        self.db.delete_table(table_name)

        # Also delete the configuration
        self.db.delete_table_config(table_name)

    def create_table_from_config(
        self,
        config: Dict[str, Any],
        table_name: Optional[str] = None,
    ) -> Union["Table", Dict[str, "Table"]]:
        """
        Create table(s) from configuration.

        Args:
            config: Configuration dictionary with table definitions
                    Format: {"table_name": {"col1": "str", "col2": "int", "id": "auto"}, ...}
            table_name: If provided, only create this specific table from the config

        Returns:
            Table instance if table_name is provided, otherwise dictionary of table_name -> Table

        Example:
            config = {
                "users": {
                    "name": "str",
                    "email": "str",
                    "age": int,
                    "id": "auto"
                },
                "posts": {
                    "title": "str",
                    "content": "str",
                    "id": "auto"
                }
            }

            # Create all tables
            table = client.create_table_from_config(config)

            # Create only 'users' table
            table = client.create_table_from_config(config, table_name="users")
        """

        if table_name is not None:
            # Create single table
            if table_name not in config:
                raise KeyError(f"Table '{table_name}' not found in config")

            table_config = config[table_name]
            self.db.create_table_from_config(table_name, table_config)
            return Table(self.db, table_name)
        else:
            # Create all tables
            table = {}
            for name, table_config in config.items():
                self.db.create_table_from_config(name, table_config)
                table[name] = Table(self.db, name)
            return table

    def get_table_from_config(
        self,
        config: Dict[str, Any],
        table_name: str,
    ) -> "Table":
        """
        Get a table instance from configuration.

        This method retrieves an existing table. It doesn't create the table if it doesn't exist.

        Args:
            config: Configuration dictionary (for reference/validation)
            table_name: Name of the table to retrieve

        Returns:
            Table instance

        Raises:
            TableNotFoundError: If table doesn't exist

        Example:
            config = {
                "users": {
                    "name": "str",
                    "email": "str"
                }
            }
            table = client.get_table_from_config(config, "users")
        """
        
        if not self.db.table_exists(table_name):
            raise TableNotFoundError(f"Table '{table_name}' not found")

        return Table(self.db, table_name)

    def delete_table_from_config(
        self,
        config: Dict[str, Any],
        table_name: str,
    ) -> None:
        """
        Delete a table and its configuration.

        Args:
            config: Configuration dictionary (for reference)
            table_name: Name of the table to delete

        Raises:
            TableNotFoundError: If table doesn't exist

        Example:
            config = {
                "users": {
                    "name": "str",
                    "email": "str"
                }
            }
            client.delete_table_from_config(config, "users")
        """

        if table_name not in config:
            raise KeyError(f"Table '{table_name}' not found in config")

        if not self.db.table_exists(table_name):
            raise TableNotFoundError(f"Table '{table_name}' not found")

        self.db.delete_table(table_name)
        self.db.delete_table_config(table_name)

    def close(
        self,
    ) -> None:
        """
        Close database connection.
        """

        self.db.close()
