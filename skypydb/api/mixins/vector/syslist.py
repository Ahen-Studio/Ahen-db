"""
Module containing the SysList class, which is used to list all collections in the database.
"""

from typing import (
    List,
    TYPE_CHECKING
)

if TYPE_CHECKING:
    from skypydb.api.collection import Collection

class SysList:
    def list_collections(
        self
    ) -> List["Collection"]:
        """
        List all collections in the database.

        Returns:
            List of Collection instances

        Example:
            for collection in client.list_collections():
                print(f"Collection: {collection.name}")
                print(f"Documents: {collection.count()}")
        """

        collections = []

        for collection_info in self._db.list_collections():
            name = collection_info["name"]
            # use cached instance if available
            if name in self._collections:
                collections.append(self._collections[name])
            else:
                from skypydb.api.collection import Collection
                collection = Collection(
                    db=self._db,
                    name=name,
                    metadata=collection_info.get("metadata")
                )
                self._collections[name] = collection
                collections.append(collection)
        return collections