"""
Module containing the SysCreate class, which is used to create a table.
"""

import importlib
import importlib.util
import os
from typing import Dict
from skypydb.errors import TableAlreadyExistsError
from skypydb.table.table import Table
from skypydb.schema import SysSchema

class SysCreate:
    def create_table(self) -> Dict[str, Table]:
        """
        Create all tables defined in db/schema.py.

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

        # first, try to load schema.py from the current working directory (./db/schema.py)
        cwd_schema_path = os.path.join(os.getcwd(), "db", "schema.py")
        if os.path.exists(cwd_schema_path):
            spec = importlib.util.spec_from_file_location("skypydb._user_schema", cwd_schema_path)
            if spec and spec.loader:
                module = importlib.util.module_from_spec(spec)
                spec.loader.exec_module(module)
                schema = getattr(module, "schema", None)
        # if not found in cwd, try to load from the installed package location
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
        if not isinstance(schema, SysSchema):
            raise ValueError(
                f"Expected a Schema object, got {type(schema).__name__}"
            )
        # create all tables from schema
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
            # create table with schema definition
            self.db.create_table(table_name, table_def)
            created_tables[table_name] = Table(self.db, table_name)
        return created_tables