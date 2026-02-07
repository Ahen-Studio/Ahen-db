"""
Module containing the SysUpdate class, which is used to update items in the collection.
"""

import json
from typing import (
    Dict,
    List,
    Optional,
    Any
)
from skypydb.security.validation import InputValidator

class SysUpdate:
    def update(
        self,
        collection_name: str,
        ids: List[str],
        embeddings: Optional[List[List[float]]] = None,
        documents: Optional[List[str]] = None,
        metadatas: Optional[List[Dict[str, Any]]] = None
    ) -> None:
        """
        Update items in a collection.

        Args:
            collection_name: Name of the collection
            ids: List of IDs to update
            embeddings: Optional new embeddings
            documents: Optional new documents (will be embedded)
            metadatas: Optional new metadata
        """

        collection_name = InputValidator.validate_table_name(collection_name)
        if not self.collection_exists(collection_name):
            raise ValueError(f"Collection '{collection_name}' not found")
        if embeddings is None and documents is not None:
            if self.embedding_function is None:
                raise ValueError(
                    "Documents provided but no embedding function set."
                )
            embeddings = self.embedding_function(documents)

        cursor = self.conn.cursor()

        for i, item_id in enumerate(ids):
            updates = []
            params = []
            if embeddings is not None:
                updates.append("embedding = ?")
                params.append(json.dumps(embeddings[i]))
            if documents is not None:
                updates.append("document = ?")
                params.append(documents[i])
            if metadatas is not None:
                updates.append("metadata = ?")
                params.append(json.dumps(metadatas[i]) if metadatas[i] else None)
            if updates:
                params.append(item_id)
                cursor.execute(
                    f"UPDATE [vec_{collection_name}] SET {', '.join(updates)} WHERE id = ?",
                    params
                )
        self.conn.commit()