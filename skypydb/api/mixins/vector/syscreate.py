"""
Module containing the SysCreate class, which is used to create a collection.
"""

from typing import (
    Any,
    Dict,
    Optional,
    TYPE_CHECKING
)

if TYPE_CHECKING:
    from skypydb.api.collection import Collection

class SysCreate:
    def create_collection(
        self,
        name: str,
        metadata: Optional[Dict[str, Any]] = None,
        get_or_create: bool = False
    ) -> "Collection":
        """
        Create a new collection.

        A collection is a named group of documents with their embeddings
        and metadata. Each document in the collection has a unique ID.

        Args:
            name: Unique name for the collection
            metadata: Optional metadata to attach to the collection
            get_or_create: If True, return existing collection if it exists

        Returns:
            Collection instance

        Raises:
            ValueError: If collection already exists and get_or_create is False

        Example:
            # Create a new collection
            collection = client.create_collection("articles")
            
            # Create or get existing
            collection = client.create_collection(
                "articles",
                get_or_create=True
            )
        """

        if get_or_create:
            # Ensure the collection exists, creating it if necessary.
            # We intentionally discard the returned instance here so that
            # this method can apply a consistent caching strategy via
            # self._collections below.
            self.get_or_create_collection(name, metadata)
        else:
            # Create collection in database
            self._db.create_collection(name, metadata)

        # Create and cache collection instance (or return cached one)
        _collection = self._collections.get(name)
        if _collection is None:
            from skypydb.api.collection import Collection
            _collection = Collection(
                db=self._db,
                name=name,
                metadata=metadata
            )
            self._collections[name] = _collection
        return _collection