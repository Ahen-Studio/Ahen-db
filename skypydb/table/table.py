"""
Table API for managing data.
"""

from typing import Any, Dict, List, Optional
from ..db.database import Database
from ..errors import TableNotFoundError


# main class for managing table
class Table:
    """
    Represents a table in the database.
    """

    def __init__(
        self,
        db: Database,
        table_name: str,
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


    # add data to the table
    def add(
        self,
        **kwargs,
    ) -> List[str]:
        """
        Add data to the table.

        Each keyword argument can be a single value or a list.
        If a value is a list, multiple rows will be inserted.
        IDs and timestamps are automatically generated.

        Args:
            **kwargs: Column names and values (can be lists)

        Returns:
            List of IDs for inserted rows

        Example:
            table.add(
                title=["doc1", "doc2"],
                user_id=["user123"],
                content=["content1", "content2"]
            )
        """

        # Handle "auto" for id field
        if 'id' in kwargs:
            if kwargs['id'] == ['auto'] or kwargs['id'] == 'auto':
                del kwargs['id']

        # Determine number of rows to insert
        max_length = 1
        for key, value in kwargs.items():
            if isinstance(value, list):
                if not value:
                    raise ValueError(f"Empty list provided for '{key}'")
                max_length = max(max_length, len(value))

        # Prepare data for each row
        inserted_ids = []
        for i in range(max_length):
            row_data = {}
            for key, value in kwargs.items():
                if isinstance(value, list):
                    row_data[key] = value[i] if i < len(value) else value[-1]
                else:
                    row_data[key] = value

            # Validate data against table config
            validated_data = self.db.validate_data_with_config(self.table_name, row_data)

            # Insert row with validated data
            row_id = self.db.insert_data(self.table_name, validated_data, generate_id=True)

            inserted_ids.append(row_id)

        return inserted_ids


    # delete a data from the table
    def delete(
        self,
        **filters,
    ) -> int:
        """
        Delete data from the table based on filters.

        Args:
            **filters: Filters as keyword arguments (column name = value or list of values)

        Returns:
            Number of rows deleted

        Example:
            # Delete by ID
            table.delete(
                id="123"
            )

            # Delete by multiple criteria
            table.delete(
                user_id="user123",
                title="document"
            )

            # Delete with list values (uses IN clause)
            table.delete(
                title=["doc1", "doc2"]
            )
        """

        # Convert list filters if needed
        processed_filters = {}
        for key, value in filters.items():
            processed_filters[key] = value

        return self.db.delete_data(self.table_name, **processed_filters)


    # search a data from the table
    def search(
        self,
        index: Optional[str] = None,
        **filters,
    ) -> List[Dict[str, Any]]:
        """
        Search for data in the table.

        Args:
            index: Value to search for in the index column (primary search key)
            **filters: Additional filters as keyword arguments

        Returns:
            List of dictionaries containing matching rows

        Example:
            results = table.search(
                index="user123",
                title="document"
            )
        """

        # Convert list filters to single values (take first element)
        processed_filters = {}
        for key, value in filters.items():
            if isinstance(value, list) and len(value) > 0:
                processed_filters[key] = value[0]
            else:
                processed_filters[key] = value

        return self.db.search(self.table_name, index=index, **processed_filters)


    # get_all the data from the table
    def get_all(
        self,
    ) -> List[Dict[str, Any]]:
        """
        Get all data from the table.
        """

        return self.db.get_all_data(self.table_name)
