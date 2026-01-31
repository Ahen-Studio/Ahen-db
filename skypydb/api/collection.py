"""
Collection class for managing vector collections.
"""

from typing import Any, Dict, List, Optional
from ..db.vector_database import VectorDatabase


# main class for managing vector collections
class Collection:
    """
    Represents a vector collection in the database.

    Example:
        from skypydb import Vector_Client

        # Create a client
        client = Vector_Client()

        # Create a collection
        collection = client.create_collection("my-documents")

        # Add documents (automatically embedded using Ollama)
        collection.add(
            documents=["This is document1", "This is document2"],
            metadatas=[{"source": "notion"}, {"source": "google-docs"}],
            ids=["doc1", "doc2"]
        )

        # Query for similar documents
        results = collection.query(
            query_texts=["This is a query document"],
            n_results=2
        )
    """

    def __init__(
        self,
        db: VectorDatabase,
        name: str,
        metadata: Optional[Dict[str, Any]] = None,
    ):
        """
        Initialize collection.

        Args:
            db: VectorDatabase instance
            name: Name of the collection
            metadata: Optional collection metadata
        """

        self._db = db
        self._name = name
        self._metadata = metadata or {}


    # get the collection name
    @property
    def name(self) -> str:
        """
        Get collection name.
        """

        return self._name


    # get the collection metadata
    @property
    def metadata(self) -> Dict[str, Any]:
        """
        Get collection metadata.
        """

        return self._metadata


    # add items to the collection
    def add(
        self,
        ids: List[str],
        embeddings: Optional[List[List[float]]] = None,
        documents: Optional[List[str]] = None,
        metadatas: Optional[List[Dict[str, Any]]] = None,
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
            metadatas=metadatas,
        )


    # update existing items in the collection
    def update(
        self,
        ids: List[str],
        embeddings: Optional[List[List[float]]] = None,
        documents: Optional[List[str]] = None,
        metadatas: Optional[List[Dict[str, Any]]] = None,
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
            metadatas=metadatas,
        )


    # get items from the collection by ID or by filter
    def get(
        self,
        ids: Optional[List[str]] = None,
        where: Optional[Dict[str, Any]] = None,
        where_document: Optional[Dict[str, str]] = None,
        include: Optional[List[str]] = None,
        limit: Optional[int] = None,
        offset: Optional[int] = None,
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
            include=include,
        )

        # Apply limit and offset
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


    # query the database collection for similar items
    def query(
        self,
        query_embeddings: Optional[List[List[float]]] = None,
        query_texts: Optional[List[str]] = None,
        n_results: int = 10,
        where: Optional[Dict[str, Any]] = None,
        where_document: Optional[Dict[str, str]] = None,
        include: Optional[List[str]] = None,
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
            include=include,
        )


    # delete items from the collection by ID or by filter
    def delete(
        self,
        ids: Optional[List[str]] = None,
        where: Optional[Dict[str, Any]] = None,
        where_document: Optional[Dict[str, str]] = None,
    ) -> None:
        """
        Delete items from the collection.

        Args:
            ids: Optional list of IDs to delete
            where: Optional metadata filter
            where_document: Optional document content filter

        Example:
            # Delete by ID
            collection.delete(ids=["doc1", "doc2"])

            # Delete by metadata filter
            collection.delete(where={"source": "old"})

            # Delete by document content
            collection.delete(where_document={"$contains": "deprecated"})
        """

        self._db.delete(
            collection_name=self._name,
            ids=ids,
            where=where,
            where_document=where_document,
        )
