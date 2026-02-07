"""
Module containing the Utils class, which is used to do a bunch of stuff related to get operation in a collection.
"""

from typing import (
    Any,
    Dict,
    List
)

class Utils:
    @property
    def name(self) -> str:
        """
        Get collection name.
        """

        return self._name

    @property
    def metadata(self) -> Dict[str, Any]:
        """
        Get collection metadata.
        """

        return self._metadata
    
    def count(self) -> int:
        """
        Count the number of items in the collection.

        Returns:
            Number of items in the collection

        Example:
            print(f"Collection has {collection.count()} items")
        """

        return self._db.count(self._name)

    def peek(
        self,
        limit: int = 10
    ) -> Dict[str, List[Any]]:
        """
        Get a sample of items from the collection.

        Args:
            limit: Maximum number of items to return (default: 10)

        Returns:
            Dictionary with sample items

        Example:
            sample = collection.peek(5)
            print(f"Sample IDs: {sample['ids']}")
        """

        return self.get(limit=limit)