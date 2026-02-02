"""
Vector Client API for Skypydb.
"""

import os
import time
from typing import Any, Dict, List, Optional
from skypydb.database.vector_database import VectorDatabase
from skypydb.embeddings.ollama import OllamaEmbedding
from skypydb.api.collection import Collection


# main class for the vector client
class VectorClient:
    """
    Vector database client with ChromaDB-compatible API.

    This client provides automatic embedding generation using Ollama models.

    Example:
        import skypydb

        # Create a client
        client = skypydb.VectorClient()

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


    # initialize a new vector client
    def __init__(
        self,
        path: Optional[str] = None,
        embedding_model: str = "mxbai-embed-large",
        ollama_base_url: str = "http://localhost:11434",
    ):
        """
        Initialize Vector Client.

        The database is stored locally using SQLite. You can optionally
        configure an embedding function, or it will default to using
        Ollama with the specified model.

        Args:
            path: Path to the database directory. Defaults to ./db/_generated/vector.db
            embedding_model: Ollama model to use for embeddings (default: mxbai-embed-large)
            ollama_base_url: Base URL for Ollama API (default: http://localhost:11434)

        Example:
            # Basic usage with defaults
            client = skypydb.VectorClient()

            # With custom embedding model
            client = skypydb.VectorClient(embedding_model="mxbai-embed-large")
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

        # Set up embedding function
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


    # create a new collection
    def create_collection(
        self,
        name: str,
        metadata: Optional[Dict[str, Any]] = None,
        get_or_create: bool = False,
    ) -> Collection:
        """
        Create a new collection.

        A collection is a named group of documents with their embeddings
        and metadata. Each document in the collection has a unique ID.

        Args:
            name: Unique name for the collection
            metadata: Optional metadata to attach to the collection
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
            # Ensure the collection exists, creating it if necessary.
            # We intentionally discard the returned instance here so that
            # this method can apply a consistent caching strategy via
            # self._collections below.
            self.get_or_create_collection(name, metadata)
        else:
            # Create collection in database
            self._db.create_collection(name, metadata)

        # Create and cache collection instance (or return cached one)
        _collection = self._collections.get(name)
        if _collection is None:
            _collection = Collection(
                db=self._db,
                name=name,
                metadata=metadata,
            )
            self._collections[name] = _collection

        return _collection


    # get a collection
    def get_collection(
        self,
        name: str,
    ) -> Collection:
        """
        Get an existing collection by name.

        Args:
            name: Name of the collection to retrieve

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


    # get or create a collection
    def get_or_create_collection(
        self,
        name: str,
        metadata: Optional[Dict[str, Any]] = None,
    ) -> Collection:
        """
        Get an existing collection or create a new one.

        Args:
            name: Name of the collection
            metadata: Optional metadata (used only when creating)

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


    # list all collections present in the database
    def list_collections(
        self,
    ) -> List[Collection]:
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


    # delete a specific collection and all its data
    def delete_collection(
        self,
        name: str,
    ) -> None:
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


    # reset the database by deleting all collections
    def reset(
        self,
    ) -> bool:
        """
        Reset the database by deleting all collections.

        Returns:
            True if reset was successful

        Example:
            client.reset()
        """

        # Prefer a single backend operation if available
        reset_method = None

        if hasattr(self._db, "reset"):
            reset_method = getattr(self._db, "reset")
        elif hasattr(self._db, "clear"):
            reset_method = getattr(self._db, "clear")

        if callable(reset_method):
            reset_method()
        else:
            for collection_info in self._db.list_collections():
                self._db.delete_collection(collection_info["name"])

        self._collections.clear()
        return True


    # check if the database is alive
    def heartbeat(
        self,
    ) -> int:
        """
        Check if the database is alive.

        Returns:
            Current timestamp in nanoseconds

        Example:
            if client.heartbeat():
                print("Database is alive")
        """

        return int(time.time() * 1e9)


    # close the database connection
    def close(
        self,
    ) -> None:
        """
        Close the database connection.

        Example:
            client.close()
        """

        self._db.close()
        self._collections.clear()
