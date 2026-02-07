"""
Module containing the SysQuery class, which is used to query a collection to get similar items.
"""

from typing import (
    Dict,
    List,
    Optional,
    Any
)
from skypydb.security.validation import InputValidator
from skypydb.database.mixins.vector.utils import cosine_similarity

class SysQuery:
    def query(
        self,
        collection_name: str,
        query_embeddings: Optional[List[List[float]]] = None,
        query_texts: Optional[List[str]] = None,
        n_results: int = 10,
        where: Optional[Dict[str, Any]] = None,
        where_document: Optional[Dict[str, str]] = None,
        include: Optional[List[str]] = None
    ) -> Dict[str, List[List[Any]]]:
        """
        Query a collection for similar items.
        
        Args:
            collection_name: Name of the collection
            query_embeddings: Optional list of query embeddings
            query_texts: Optional list of query texts (will be embedded)
            n_results: Number of results to return per query
            where: Optional metadata filter
            where_document: Optional document filter
            include: Optional list of fields to include
            
        Returns:
            Dictionary with nested lists of results for each query
        """
        
        collection_name = InputValidator.validate_table_name(collection_name)
        if not self.collection_exists(collection_name):
            raise ValueError(f"Collection '{collection_name}' not found")
        if query_embeddings is None and query_texts is None:
            raise ValueError("Either query_embeddings or query_texts must be provided")
        if query_embeddings is None:
            if self.embedding_function is None:
                raise ValueError(
                    "Query texts provided but no embedding function set."
                )
            if query_texts is None:
                raise ValueError("Either query_embeddings or query_texts must be provided")
            query_embeddings = self.embedding_function(query_texts)

        include = include or ["embeddings", "documents", "metadatas", "distances"]

        # get all items from collection
        all_items = self._get_all_items(collection_name)

        results = {
            "ids": [],
            "embeddings": [] if "embeddings" in include else None,
            "documents": [] if "documents" in include else None,
            "metadatas": [] if "metadatas" in include else None,
            "distances": [] if "distances" in include else None,
        }

        for query_embedding in query_embeddings:
            # calculate similarities and filter
            scored_items = []

            for item in all_items:
                # apply filters
                if not self._matches_filters(item, where, where_document):
                    continue

                # calculate cosine similarity
                similarity = cosine_similarity(query_embedding, item["embedding"])
                # convert to distance (1 - similarity, so lower is better)
                distance = 1.0 - similarity

                scored_items.append((item, distance))

            # sort by distance (ascending) and take top n
            scored_items.sort(key=lambda x: x[1])
            top_items = scored_items[:n_results]

            # extract results for this query
            query_ids = []
            query_embeddings_result = []
            query_documents = []
            query_metadatas = []
            query_distances = []

            for item, distance in top_items:
                query_ids.append(item["id"])
                query_embeddings_result.append(item["embedding"])
                query_documents.append(item["document"])
                query_metadatas.append(item["metadata"])
                query_distances.append(distance)

            results["ids"].append(query_ids)
            if results["embeddings"] is not None:
                results["embeddings"].append(query_embeddings_result)
            if results["documents"] is not None:
                results["documents"].append(query_documents)
            if results["metadatas"] is not None:
                results["metadatas"].append(query_metadatas)
            if results["distances"] is not None:
                results["distances"].append(query_distances)
        return results