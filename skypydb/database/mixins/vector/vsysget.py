"""
Module containing the VSysGet class, which is used to get items in the collection.
"""

import json
from typing import (
    Any,
    Dict,
    List,
    Optional
)
from skypydb.security.validation import InputValidator

class VSysGet:
    def get(
        self,
        collection_name: str,
        ids: Optional[List[str]] = None,
        where: Optional[Dict[str, Any]] = None,
        where_document: Optional[Dict[str, str]] = None,
        include: Optional[List[str]] = None
    ) -> Dict[str, List[Any]]:
        """
        Get items from a collection by ID or filter.
        
        Args:
            collection_name: Name of the collection
            ids: Optional list of IDs to retrieve
            where: Optional metadata filter
            where_document: Optional document filter
            include: Optional list of fields to include (embeddings, documents, metadatas)
            
        Returns:
            Dictionary with lists of ids, embeddings, documents, and metadatas
        """

        collection_name = InputValidator.validate_table_name(collection_name)
        if not self.collection_exists(collection_name):
            raise ValueError(f"Collection '{collection_name}' not found")

        include = include or ["embeddings", "documents", "metadatas"]

        cursor = self.conn.cursor()

        if ids is not None:
            placeholders = ", ".join(["?" for _ in ids])
            cursor.execute(
                f"SELECT * FROM [vec_{collection_name}] WHERE id IN ({placeholders})",
                list(ids)
            )
        else:
            cursor.execute(f"SELECT * FROM [vec_{collection_name}]")

        results = {
            "ids": [],
            "embeddings": [] if "embeddings" in include else None,
            "documents": [] if "documents" in include else None,
            "metadatas": [] if "metadatas" in include else None,
        }
        for row in cursor.fetchall():
            item = {
                "id": row["id"],
                "document": row["document"],
                "embedding": json.loads(row["embedding"]),
                "metadata": json.loads(row["metadata"]) if row["metadata"] else None,
            }
            # apply filters
            if not self._matches_filters(item, where, where_document):
                continue

            results["ids"].append(item["id"])
            if results["embeddings"] is not None:
                results["embeddings"].append(item["embedding"])
            if results["documents"] is not None:
                results["documents"].append(item["document"])
            if results["metadatas"] is not None:
                results["metadatas"].append(item["metadata"])
        return results

    def _get_all_items(
        self,
        collection_name: str
    ) -> List[Dict[str, Any]]:
        """
        Get all items from a collection.
        """

        cursor = self.conn.cursor()
        
        cursor.execute(f"SELECT * FROM [vec_{collection_name}]")

        items = []
        for row in cursor.fetchall():
            items.append({
                "id": row["id"],
                "document": row["document"],
                "embedding": json.loads(row["embedding"]),
                "metadata": json.loads(row["metadata"]) if row["metadata"] else None,
                "created_at": row["created_at"]
            })
        return items