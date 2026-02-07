"""
Module containing the SysUpdate class, which is used to update existing items in a collection.
"""

from typing import (
    Any,
    Dict,
    Optional,
    List
)

class SysUpdate:
    def update(
        self,
        ids: List[str],
        embeddings: Optional[List[List[float]]] = None,
        documents: Optional[List[str]] = None,
        metadatas: Optional[List[Dict[str, Any]]] = None
    ) -> None:
        """
        Update existing items in the collection.

        Args:
            ids: IDs of items to update
            embeddings: Optional new embeddings
            documents: Optional new documents (will be re-embedded)
            metadatas: Optional new metadata

        Example:
            # Update metadata only
            collection.update(
                ids=["doc1"],
                metadatas=[{"lang": "fr", "updated": True}]
            )
        """

        self._db.update(
            collection_name=self._name,
            ids=ids,
            embeddings=embeddings,
            documents=documents,
            metadatas=metadatas
        )