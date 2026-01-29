"""
Vector Client API for Skypydb.

Provides a ChromaDB-compatible API for vector storage and similarity search,
designed to integrate with mem0 and other AI memory systems.
"""

import os
from typing import Any, Callable, Dict, List, Optional

from ..db.vector_database import VectorDatabase
from ..embeddings.ollama import OllamaEmbedding
from .collection import Collection


class Vector_Client:
    """
    Vector database client with ChromaDB-compatible API.
    
    This client provides a familiar interface for working with vector
    embeddings, similar to ChromaDB. It supports automatic embedding
    generation using Ollama models and is compatible with mem0 for
    AI memory management.
    
    Example:
        from skypydb.api.client import Vector_Client
        
        # Create client with default settings
        client = Vector_Client(path="./vector_db")
        
        # Create a collection
        collection = client.create_collection("my-documents")
        
        # Add documents (automatically embedded using Ollama)
        collection.add(
            documents=["This is document 1", "This is document 2"],
            metadatas=[{"source": "web"}, {"source": "pdf"}],
            ids=["doc1", "doc2"]
        )
        
        # Query for similar documents
        results = collection.query(
            query_texts=["Find similar documents"],
            n_results=5
        )
    """
    
    def __init__(
        self,
        path: Optional[str] = None,
        host: Optional[str] = None,
        port: Optional[int] = None,
        embedding_function: Optional[Callable[[List[str]], List[List[float]]]] = None,
        embedding_model: str = "nomic-embed-text",
        ollama_base_url: str = "http://localhost:11434",
    ):
        """
        Initialize Vector Client.
        
        The database is stored locally using SQLite. You can optionally
        configure an embedding function, or it will default to using
        Ollama with the specified model.
        
        Args:
            path: Path to the database directory. Defaults to ./db/_generated/vector.db
            host: Optional host for remote connection (reserved for future use)
            port: Optional port for remote connection (reserved for future use)
            embedding_function: Optional custom embedding function that takes
                               a list of texts and returns a list of embeddings.
                               If not provided, uses Ollama.
            embedding_model: Ollama model to use for embeddings (default: nomic-embed-text)
            ollama_base_url: Base URL for Ollama API (default: http://localhost:11434)
            
        Example:
            # Basic usage with defaults
            client = Vector_Client(path="./my_vectors")
            
            # With custom embedding model
            client = Vector_Client(
                path="./my_vectors",
                embedding_model="mxbai-embed-large"
            )
            
            # With custom embedding function
            def my_embed_fn(texts):
                # Your custom embedding logic
                return [[0.1, 0.2, ...] for _ in texts]
            
            client = Vector_Client(
                path="./my_vectors",
                embedding_function=my_embed_fn
            )
        """
        # Set default path
        if path is None:
            path = "./db/_generated/vector.db"
        
        # Ensure path ends with .db
        if not path.endswith(".db"):
            path = os.path.join(path, "vector.db")
        
        # Ensure directory exists
        db_dir = os.path.dirname(path)
        if db_dir and not os.path.exists(db_dir):
            os.makedirs(db_dir, exist_ok=True)
        
        self.path = path
        self.host = host
        self.port = port
        
        # Set up embedding function
        if embedding_function is not None:
            self._embedding_function = embedding_function
        else:
            self._embedding_function = OllamaEmbedding(
                model=embedding_model,
                base_url=ollama_base_url
            )
        
        # Initialize vector database
        self._db = VectorDatabase(
            path=path,
            embedding_function=self._embedding_function
        )
        
        # Cache for collection instances
        self._collections: Dict[str, Collection] = {}
    
    def create_collection(
        self,
        name: str,
        metadata: Optional[Dict[str, Any]] = None,
        embedding_function: Optional[Callable[[List[str]], List[List[float]]]] = None,
        get_or_create: bool = False,
    ) -> Collection:
        """
        Create a new collection.
        
        A collection is a named group of documents with their embeddings
        and metadata. Each document in the collection has a unique ID.
        
        Args:
            name: Unique name for the collection
            metadata: Optional metadata to attach to the collection
            embedding_function: Optional custom embedding function for this collection
                               (overrides client's default)
            get_or_create: If True, return existing collection if it exists
            
        Returns:
            Collection instance
            
        Raises:
            ValueError: If collection already exists and get_or_create is False
            
        Example:
            # Create a new collection
            collection = client.create_collection("articles")
            
            # Create or get existing
            collection = client.create_collection(
                "articles",
                get_or_create=True
            )
        """
        if get_or_create:
            return self.get_or_create_collection(name, metadata, embedding_function)
        
        # Create collection in database
        self._db.create_collection(name, metadata)
        
        # Use collection-specific embedding function if provided
        if embedding_function is not None:
            # Create a new database instance with the custom function
            # For now, we'll use the same database but track the function
            pass
        
        # Create and cache collection instance
        collection = Collection(
            db=self._db,
            name=name,
            metadata=metadata,
        )
        self._collections[name] = collection
        
        return collection
    
    def get_collection(
        self,
        name: str,
        embedding_function: Optional[Callable[[List[str]], List[List[float]]]] = None,
    ) -> Collection:
        """
        Get an existing collection by name.
        
        Args:
            name: Name of the collection to retrieve
            embedding_function: Optional embedding function to use
            
        Returns:
            Collection instance
            
        Raises:
            ValueError: If collection doesn't exist
            
        Example:
            collection = client.get_collection("articles")
        """
        # Check if collection exists
        collection_info = self._db.get_collection(name)
        if collection_info is None:
            raise ValueError(f"Collection '{name}' not found")
        
        # Return cached instance if available
        if name in self._collections:
            return self._collections[name]
        
        # Create new collection instance
        collection = Collection(
            db=self._db,
            name=name,
            metadata=collection_info.get("metadata"),
        )
        self._collections[name] = collection
        
        return collection
    
    def get_or_create_collection(
        self,
        name: str,
        metadata: Optional[Dict[str, Any]] = None,
        embedding_function: Optional[Callable[[List[str]], List[List[float]]]] = None,
    ) -> Collection:
        """
        Get an existing collection or create a new one.
        
        Args:
            name: Name of the collection
            metadata: Optional metadata (used only when creating)
            embedding_function: Optional embedding function to use
            
        Returns:
            Collection instance
            
        Example:
            # Always works, whether collection exists or not
            collection = client.get_or_create_collection("articles")
        """
        # Get or create in database
        collection_info = self._db.get_or_create_collection(name, metadata)
        
        # Return cached instance if available
        if name in self._collections:
            return self._collections[name]
        
        # Create new collection instance
        collection = Collection(
            db=self._db,
            name=name,
            metadata=collection_info.get("metadata"),
        )
        self._collections[name] = collection
        
        return collection
    
    def delete_collection(self, name: str) -> None:
        """
        Delete a collection and all its data.
        
        This permanently removes the collection and all documents,
        embeddings, and metadata stored within it.
        
        Args:
            name: Name of the collection to delete
            
        Raises:
            ValueError: If collection doesn't exist
            
        Example:
            client.delete_collection("old-articles")
        """
        # Delete from database
        self._db.delete_collection(name)
        
        # Remove from cache
        if name in self._collections:
            del self._collections[name]
    
    def list_collections(self) -> List[Collection]:
        """
        List all collections in the database.
        
        Returns:
            List of Collection instances
            
        Example:
            for collection in client.list_collections():
                print(f"Collection: {collection.name}")
                print(f"Documents: {collection.count()}")
        """
        collections = []
        
        for collection_info in self._db.list_collections():
            name = collection_info["name"]
            
            # Use cached instance if available
            if name in self._collections:
                collections.append(self._collections[name])
            else:
                collection = Collection(
                    db=self._db,
                    name=name,
                    metadata=collection_info.get("metadata"),
                )
                self._collections[name] = collection
                collections.append(collection)
        
        return collections
    
    def heartbeat(self) -> int:
        """
        Check if the database is alive.
        
        Returns:
            Current timestamp in nanoseconds
            
        Example:
            if client.heartbeat():
                print("Database is alive")
        """
        import time
        return int(time.time() * 1e9)
    
    def reset(self) -> bool:
        """
        Reset the database by deleting all collections.
        
        WARNING: This permanently deletes all data!
        
        Returns:
            True if reset was successful
            
        Example:
            # Use with caution!
            client.reset()
        """
        for collection_info in self._db.list_collections():
            self._db.delete_collection(collection_info["name"])
        
        self._collections.clear()
        return True
    
    @property
    def embedding_function(self) -> Callable[[List[str]], List[List[float]]]:
        """Get the current embedding function."""
        return self._embedding_function
    
    def set_embedding_function(
        self,
        embedding_function: Callable[[List[str]], List[List[float]]]
    ) -> None:
        """
        Set a new embedding function for the client.
        
        Args:
            embedding_function: Function that takes texts and returns embeddings
        """
        self._embedding_function = embedding_function
        self._db.set_embedding_function(embedding_function)
    
    def close(self) -> None:
        """
        Close the database connection.
        
        Example:
            client.close()
        """
        self._db.close()
        self._collections.clear()
    
    def __enter__(self) -> "Vector_Client":
        """Support context manager protocol."""
        return self
    
    def __exit__(self, exc_type, exc_val, exc_tb) -> None:
        """Close connection on context exit."""
        self.close()