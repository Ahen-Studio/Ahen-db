"""
Module containing the SysSchema class, which is used to perform get operations on the schema in the database.
"""

from typing import (
    List,
    Dict,
    Optional,
    TYPE_CHECKING
)

if TYPE_CHECKING:
    from skypydb.schema.schema import TableDefinition

class SysSchema:
    def __init__(
        self,
        tables: Dict[str, "TableDefinition"]
    ):
        self.tables = tables

    def get_table_definition(
        self,
        table_name: str
    ) -> Optional["TableDefinition"]:
        """
        Get a table definition by name.

        Args:
            table_name: Name of the table

        Returns:
            TableDefinition if found, None otherwise
        """

        return self.tables.get(table_name)

    def get_all_table_names(self) -> List[str]:
        """
        Get all table names in the schema.

        Returns:
            List of table names
        """

        return list(self.tables.keys())
