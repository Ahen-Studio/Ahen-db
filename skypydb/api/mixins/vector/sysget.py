"""
Module containing the SysGet class, which is used to get a collection.
"""

from typing import (
    Any,
    Dict,
    Optional,
    TYPE_CHECKING
)

if TYPE_CHECKING:
    from skypydb.api.collection import Collection

class SysGet:
    def get_collection(
        self,
        name: str
    ) -> "Collection":
        """
        Get an existing collection by name.

        Args:
            name: Name of the collection to retrieve

        Returns:
            Collection instance

        Raises:
            ValueError: If collection doesn't exist

        Example:
            collection = client.get_collection("articles")
        """

        # check if collection exists
        collection_info = self._db.get_collection(name)
        if collection_info is None:
            raise ValueError(f"Collection '{name}' not found")

        # return cached instance if available
        if name in self._collections:
            return self._collections[name]

        # create new collection instance
        from skypydb.api.collection import Collection
        collection = Collection(
            db=self._db,
            name=name,
            metadata=collection_info.get("metadata")
        )
        self._collections[name] = collection
        return collection

    def get_or_create_collection(
        self,
        name: str,
        metadata: Optional[Dict[str, Any]] = None
    ) -> "Collection":
        """
        Get an existing collection or create a new one.

        Args:
            name: Name of the collection
            metadata: Optional metadata (used only when creating)

        Returns:
            Collection instance

        Example:
            # Always works, whether collection exists or not
            collection = client.get_or_create_collection("articles")
        """

        # get or create in database
        collection_info = self._db.get_or_create_collection(name, metadata)

        # return cached instance if available
        if name in self._collections:
            return self._collections[name]

        # create new collection instance
        from skypydb.api.collection import Collection
        collection = Collection(
            db=self._db,
            name=name,
            metadata=collection_info.get("metadata")
        )
        self._collections[name] = collection
        return collection