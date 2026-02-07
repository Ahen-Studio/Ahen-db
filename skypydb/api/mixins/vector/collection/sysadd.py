"""
Module containing the SysAdd class, which is used to add items to a collection.
"""

from typing import (
    Any,
    Dict,
    Optional,
    List
)

class SysAdd:
    def add(
        self,
        ids: List[str],
        embeddings: Optional[List[List[float]]] = None,
        documents: Optional[List[str]] = None,
        metadatas: Optional[List[Dict[str, Any]]] = None
    ) -> None:
        """
        Add items to the collection.

        Args:
            ids: Unique IDs for each item (required)
            embeddings: Optional pre-computed embedding vectors
            documents: Optional text documents to embed and store
            metadatas: Optional metadata dictionaries for each item

        Raises:
            ValueError: If neither embeddings nor documents provided,
                       or if list lengths don't match

        Example:
            # Add with automatic embedding
            collection.add(
                documents=["Hello world", "Goodbye world"],
                metadatas=[{"lang": "en"}, {"lang": "en"}],
                ids=["doc1", "doc2"]
            )

            # Add with pre-computed embeddings
            collection.add(
                embeddings=[[0.1, 0.2, ...], [0.3, 0.4, ...]],
                documents=["Hello", "Goodbye"],
                ids=["doc1", "doc2"]
            )
        """

        self._db.add(
            collection_name=self._name,
            ids=ids,
            embeddings=embeddings,
            documents=documents,
            metadatas=metadatas
        )