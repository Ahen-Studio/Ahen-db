"""
Module containing the SysGet class, which is used to get items from a collection.
"""

from typing import (
    Any,
    Dict,
    Optional,
    List
)

class SysGet:
    def get(
        self,
        ids: Optional[List[str]] = None,
        where: Optional[Dict[str, Any]] = None,
        where_document: Optional[Dict[str, str]] = None,
        include: Optional[List[str]] = None,
        limit: Optional[int] = None,
        offset: Optional[int] = None
    ) -> Dict[str, List[Any]]:
        """
        Get items from the collection by ID or filter.

        Args:
            ids: Optional list of IDs to retrieve
            where: Optional metadata filter
            where_document: Optional document content filter
            include: Optional list of fields to include
                    (embeddings, documents, metadatas)
            limit: Optional maximum number of results
            offset: Optional offset for pagination

        Returns:
            Dictionary with lists of ids, embeddings, documents, metadatas

        Example:
            # Get by IDs
            results = collection.get(ids=["doc1", "doc2"])

            # Get by filter
            results = collection.get(
                where={"source": "web"},
                include=["documents", "metadatas"]
            )

            # Access results
            for i, doc_id in enumerate(results["ids"]):
                print(f"ID: {doc_id}")
                print(f"Document: {results['documents'][i]}")
        """

        results = self._db.get(
            collection_name=self._name,
            ids=ids,
            where=where,
            where_document=where_document,
            include=include
        )
        # apply limit and offset
        if offset is not None or limit is not None:
            start = offset or 0
            end = (start + limit) if limit else None

            results["ids"] = results["ids"][start:end]
            if results.get("embeddings") is not None:
                results["embeddings"] = results["embeddings"][start:end]
            if results.get("documents") is not None:
                results["documents"] = results["documents"][start:end]
            if results.get("metadatas") is not None:
                results["metadatas"] = results["metadatas"][start:end]
        return results