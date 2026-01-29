"""
Collection class for managing vector collections.
Provides a ChromaDB-compatible API for interacting with vector data.
"""

from typing import Any, Dict, List, Optional, TYPE_CHECKING

if TYPE_CHECKING:
    from ..db.vector_database import VectorDatabase


class Collection:
    """
    Represents a vector collection in the database.
    
    Provides methods to add, update, delete, get, and query documents
    with their embeddings, following the ChromaDB API pattern.
    
    Example:
        collection = client.create_collection("my-collection")
        
        # Add documents
        collection.add(
            documents=["doc1", "doc2"],
            metadatas=[{"source": "web"}, {"source": "pdf"}],
            ids=["id1", "id2"]
        )
        
        # Query for similar documents
        results = collection.query(
            query_texts=["search query"],
            n_results=5
        )
    """
    
    def __init__(
        self,
        db: "VectorDatabase",
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
    
    @property
    def name(self) -> str:
        """Get collection name."""
        return self._name
    
    @property
    def metadata(self) -> Dict[str, Any]:
        """Get collection metadata."""
        return self._metadata
    
    def add(
        self,
        ids: List[str],
        embeddings: Optional[List[List[float]]] = None,
        documents: Optional[List[str]] = None,
        metadatas: Optional[List[Dict[str, Any]]] = None,
    ) -> None:
        """
        Add items to the collection.
        
        You must provide either embeddings or documents (or both).
        If only documents are provided, they will be automatically embedded
        using the configured embedding function.
        
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
    
    def update(
        self,
        ids: List[str],
        embeddings: Optional[List[List[float]]] = None,
        documents: Optional[List[str]] = None,
        metadatas: Optional[List[Dict[str, Any]]] = None,
    ) -> None:
        """
        Update existing items in the collection.
        
        Only the provided fields will be updated. For example, if you
        only provide new metadatas, the embeddings and documents will
        remain unchanged.
        
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
    
    def upsert(
        self,
        ids: List[str],
        embeddings: Optional[List[List[float]]] = None,
        documents: Optional[List[str]] = None,
        metadatas: Optional[List[Dict[str, Any]]] = None,
    ) -> None:
        """
        Insert or update items in the collection.
        
        If an item with the given ID exists, it will be updated.
        Otherwise, a new item will be created.
        
        Args:
            ids: IDs of items to upsert
            embeddings: Optional embeddings
            documents: Optional documents
            metadatas: Optional metadata
        """
        # The add method already uses INSERT OR REPLACE
        self._db.add(
            collection_name=self._name,
            ids=ids,
            embeddings=embeddings,
            documents=documents,
            metadatas=metadatas,
        )
    
    def delete(
        self,
        ids: Optional[List[str]] = None,
        where: Optional[Dict[str, Any]] = None,
        where_document: Optional[Dict[str, str]] = None,
    ) -> None:
        """
        Delete items from the collection.
        
        Items can be deleted by ID or by filter. If ids is provided,
        only those specific items will be deleted. Otherwise, items
        matching the filters will be deleted.
        
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
        
        Performs similarity search using cosine similarity.
        You must provide either query_embeddings or query_texts.
        
        Args:
            query_embeddings: Optional query embedding vectors
            query_texts: Optional query texts (will be embedded)
            n_results: Number of results to return per query (default: 10)
            where: Optional metadata filter to apply before search
            where_document: Optional document content filter
            include: Optional list of fields to include in results
                    (embeddings, documents, metadatas, distances)
                    
        Returns:
            Dictionary with nested lists of results for each query:
            - ids: List of lists of matching IDs
            - embeddings: List of lists of embeddings (if requested)
            - documents: List of lists of documents (if requested)
            - metadatas: List of lists of metadata (if requested)
            - distances: List of lists of distances (if requested)
            
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
    
    def count(self) -> int:
        """
        Count the number of items in the collection.
        
        Returns:
            Number of items in the collection
            
        Example:
            print(f"Collection has {collection.count()} items")
        """
        return self._db.count(self._name)
    
    def peek(self, limit: int = 10) -> Dict[str, List[Any]]:
        """
        Get a sample of items from the collection.
        
        Useful for quickly inspecting the collection contents.
        
        Args:
            limit: Maximum number of items to return (default: 10)
            
        Returns:
            Dictionary with sample items
            
        Example:
            sample = collection.peek(5)
            print(f"Sample IDs: {sample['ids']}")
        """
        return self.get(limit=limit)
    
    def modify(
        self,
        name: Optional[str] = None,
        metadata: Optional[Dict[str, Any]] = None,
    ) -> None:
        """
        Modify collection name or metadata.
        
        Note: Renaming collections is not currently supported.
        Only metadata can be modified.
        
        Args:
            name: New name for the collection (not supported)
            metadata: New metadata for the collection
        """
        if name is not None and name != self._name:
            raise NotImplementedError("Renaming collections is not supported")
        
        if metadata is not None:
            self._metadata = metadata
            # Note: This doesn't persist to the database
            # Would need to update _vector_collections table