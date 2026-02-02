"""
Client API for Skypydb.
"""

import importlib
import importlib.util
import os
from typing import Dict, Optional
from skypydb.database.database import Database
from skypydb.errors import TableAlreadyExistsError, TableNotFoundError
from skypydb.table.table import Table
from skypydb.schema import Schema

# constant to define the path to the database file
DB_PATH = "./db/_generated/skypydb.db"


# main class to interact with Skypydb
class Client:
    """
    Main client for interacting with Skypydb.
    All tables must be defined in db/schema.py using the schema system.
    """

    def __init__(
        self,
        encryption_key: Optional[str] = None,
        salt: Optional[bytes] = None,
        encrypted_fields: Optional[list] = None,
    ):
        """
        Initialize Skypydb client.

        The database is stored at ./db/_generated/skypydb.db

        Args:
            encryption_key: Optional encryption key for data encryption at rest.
                           If provided, sensitive data will be encrypted.
                           Generate a secure key with: EncryptionManager.generate_key()
            salt: Optional salt for PBKDF2HMAC; must be non-empty and provided
                                 when encryption is enabled (i.e., when an encryption_key is set).
            encrypted_fields: Optional list of field names to encrypt.
                             If None and encryption is enabled, all fields except
                             'id' and 'created_at' will be encrypted.

        Example:
            # Without encryption
            client = skypydb.Client()

            # With encryption (all fields encrypted by default)
            from skypydb.security import EncryptionManager

            key = EncryptionManager.generate_key()

            client = skypydb.Client(encryption_key=key)

            # With encryption (specific fields only)
            client = skypydb.Client(
                encryption_key=key,
                encrypted_fields=["content", "email", "password"]
            )
        """
        
        # Ensure the directory exists
        db_dir = os.path.dirname(DB_PATH)
        if db_dir and not os.path.exists(db_dir):
            os.makedirs(db_dir, exist_ok=True)

        self.path = DB_PATH
        self.db = Database(DB_PATH, encryption_key=encryption_key, salt=salt, encrypted_fields=encrypted_fields)


    # create a table
    def create_table(self) -> Dict[str, Table]:
        """
        Create all tables defined in db/schema.py.

        This method reads the schema from db/schema.py and creates all tables
        with their columns, types, and indexes as defined in the schema.

        Returns:
            Dictionary mapping table names to Table instances

        Raises:
            TableAlreadyExistsError: If any table already exists
            ValueError: If schema file is missing or invalid

        Example:
            # Define your schema in db/schema.py, then:
            client = skypydb.Client()
            tables = client.create_table()

            # Access tables
            users_table = tables["users"]
            posts_table = tables["posts"]
        """

        schema = None

        # First, try to load schema.py from the current working directory (./db/schema.py)
        cwd_schema_path = os.path.join(os.getcwd(), "db", "schema.py")
        if os.path.exists(cwd_schema_path):
            spec = importlib.util.spec_from_file_location("skypydb._user_schema", cwd_schema_path)
            if spec and spec.loader:
                module = importlib.util.module_from_spec(spec)
                spec.loader.exec_module(module)
                schema = getattr(module, "schema", None)

        # If not found in cwd, try to load from the installed package location
        if schema is None:
            package_root = os.path.dirname(os.path.dirname(__file__))
            schema_path = os.path.join(package_root, "schema.py")
            if os.path.exists(schema_path):
                spec = importlib.util.spec_from_file_location("skypydb._schema_file", schema_path)
                if spec and spec.loader:
                    module = importlib.util.module_from_spec(spec)
                    spec.loader.exec_module(module)
                    schema = getattr(module, "schema", None)

        if schema is None:
            raise ValueError(
                "No 'schema' object found in db/schema.py. "
                "Please create a schema.py file in ./db/schema.py with a schema definition "
                "using: schema = defineSchema({...})"
            )

        if not isinstance(schema, Schema):
            raise ValueError(
                f"Expected a Schema object, got {type(schema).__name__}"
            )

        # Create all tables from schema
        created_tables: Dict[str, Table] = {}
        table_names = schema.get_all_table_names()
        existing = [name for name in table_names if self.db.table_exists(name)]
        if existing:
            raise TableAlreadyExistsError(
                f"Tables already exist in the database: {', '.join(existing)}"
            )
        for table_name in table_names:
            table_def = schema.get_table_definition(table_name)
            if table_def is None:
                continue
            # Create table with schema definition
            self.db.create_table_from_schema(table_name, table_def)
            created_tables[table_name] = Table(self.db, table_name)

        return created_tables


    # delete a table
    def delete_table(
        self,
        table_name: str,
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


    # retrieve a table
    def get_table(
        self,
        table_name: str,
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


    # close the client
    def close(self) -> None:
        """
        Close database connection.

        Example:
            client.close()
        """

        self.db.close()
