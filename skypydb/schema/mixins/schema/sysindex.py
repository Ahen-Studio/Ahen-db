"""
Module containing the SysIndex class, which is used to add an index to the table definition in the database.
"""

from typing import (
    List,
    Dict,
    Any
)
from skypydb.schema.values import (
    Validator
)

class SysIndex:
    def __init__(
        self,
        columns: Dict[str, Validator]
    ):
        self.columns = columns
        self.indexes: List[Dict[str, Any]] = []

    def index(
        self,
        name: str,
        fields: List[str]
    ) -> "SysIndex":
        """
        Add an index to the table definition.

        Args:
            name: Name of the index
            fields: List of column names to index

        Returns:
            Self for method chaining
        """

        # validate that fields exist in columns
        for field in fields:
            if field not in self.columns:
                raise ValueError(
                    f"Cannot create index '{name}' on non-existent field '{field}'. "
                    f"Available fields: {list(self.columns.keys())}"
                )
        self.indexes.append({
            "name": name,
            "fields": fields
        })
        return self
