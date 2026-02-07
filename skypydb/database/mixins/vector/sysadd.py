"""
Module containing the SysAdd class, which is used to add items in the collection.
"""

import json
from datetime import datetime
from typing import (
    Dict,
    Any,
    List,
    Optional
)
from skypydb.security.validation import InputValidator

class SysAdd:
    def add(
        self,
        collection_name: str,
        ids: List[str],
        embeddings: Optional[List[List[float]]] = None,
        documents: Optional[List[str]] = None,
        metadatas: Optional[List[Dict[str, Any]]] = None
    ) -> List[str]:
        """
        Add items to a collection.

        Args:
            collection_name: Name of the collection
            ids: List of unique IDs for each item
            embeddings: Optional list of embedding vectors
            documents: Optional list of documents (will be embedded if embedding_function is set)
            metadatas: Optional list of metadata dictionaries
            
        Returns:
            List of IDs of added items
            
        Raises:
            ValueError: If neither embeddings nor documents are provided
        """

        collection_name = InputValidator.validate_table_name(collection_name)
        if not self.collection_exists(collection_name):
            raise ValueError(f"Collection '{collection_name}' not found")
        if embeddings is None and documents is None:
            raise ValueError("Either embeddings or documents must be provided")
        if embeddings is None:
            if self.embedding_function is None:
                raise ValueError(
                    "Documents provided but no embedding function set. "
                    "Either provide embeddings directly or set an embedding_function."
                )
            if documents is None:
                raise ValueError("Either embeddings or documents must be provided")
            embeddings = self.embedding_function(documents)

        # validate lengths match
        n_items = len(ids)
        if len(embeddings) != n_items:
            raise ValueError(
                f"Number of embeddings ({len(embeddings)}) doesn't match "
                f"number of IDs ({n_items})"
            )
        if documents is not None and len(documents) != n_items:
            raise ValueError(
                f"Number of documents ({len(documents)}) doesn't match "
                f"number of IDs ({n_items})"
            )
        if metadatas is not None and len(metadatas) != n_items:
            raise ValueError(
                f"Number of metadatas ({len(metadatas)}) doesn't match "
                f"number of IDs ({n_items})"
            )

        cursor = self.conn.cursor()
        
        now = datetime.now().isoformat()

        for i, item_id in enumerate(ids):
            embedding = embeddings[i]
            document = documents[i] if documents else None
            metadata = metadatas[i] if metadatas else None

            cursor.execute(
                f"""
                INSERT OR REPLACE INTO [vec_{collection_name}] 
                (id, document, embedding, metadata, created_at)
                VALUES (?, ?, ?, ?, ?)
                """,
                (
                    item_id,
                    document,
                    json.dumps(embedding),
                    json.dumps(metadata) if metadata else None,
                    now
                )
            )
        self.conn.commit()
        return ids