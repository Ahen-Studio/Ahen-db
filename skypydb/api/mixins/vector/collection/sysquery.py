"""
Module containing the SysQuery class, which is used to query the collection for similar items.
"""

from typing import (
    Any,
    Dict,
    Optional,
    List
)

class SysQuery:
    def query(
        self,
        query_embeddings: Optional[List[List[float]]] = None,
        query_texts: Optional[List[str]] = None,
        n_results: int = 10,
        where: Optional[Dict[str, Any]] = None,
        where_document: Optional[Dict[str, str]] = None,
        include: Optional[List[str]] = None
    ) -> Dict[str, List[List[Any]]]:
        """
        Query the collection for similar items.

        Args:
            query_embeddings: Optional query embedding vectors
            query_texts: Optional query texts (will be embedded)
            n_results: Number of results to return per query (default: 10)
            where: Optional metadata filter to apply before search
            where_document: Optional document content filter
            include: Optional list of fields to include in results
                    (embeddings, documents, metadatas, distances)

        Returns:
            Dictionary with nested lists of results for each query

        Example:
            # Query with text
            results = collection.query(
                query_texts=["What is machine learning?"],
                n_results=5
            )

            # Query with filters
            results = collection.query(
                query_texts=["AI applications"],
                n_results=10,
                where={"category": "technology"}
            )

            # Access results (first query's results)
            for i, doc_id in enumerate(results["ids"][0]):
                print(f"ID: {doc_id}")
                print(f"Distance: {results['distances'][0][i]}")
                print(f"Document: {results['documents'][0][i]}")
        """

        return self._db.query(
            collection_name=self._name,
            query_embeddings=query_embeddings,
            query_texts=query_texts,
            n_results=n_results,
            where=where,
            where_document=where_document,
            include=include
        )