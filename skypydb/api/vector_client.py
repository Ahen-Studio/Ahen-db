"""
Vector Client API for Skypydb.
"""

import os
from typing import Dict
from skypydb.database.vector_db import VectorDatabase
from skypydb.embeddings.ollama import OllamaEmbedding
from skypydb.api.collection import Collection
from skypydb.api.mixins.vector import (
    SysCreate,
    SysGet,
    SysList,
    SysDelete,
    Utils
)

class VectorClient(
    SysCreate,
    SysGet,
    SysList,
    SysDelete,
    Utils
):
    """
    Vector client for interacting with Skypydb.
    """

    def __init__(
        self,
        embedding_model: str = "mxbai-embed-large",
        ollama_base_url: str = "http://localhost:11434"
    ):
        """
        Initialize Vector Client.

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

        # constant to define the path to the database file
        DB_PATH = "./db/_generated/skypydb.db"

        db_dir = os.path.dirname(DB_PATH)
        if db_dir and not os.path.exists(db_dir):
            os.makedirs(db_dir, exist_ok=True)

        self.path = DB_PATH

        # set up embedding function
        self._embedding_function = OllamaEmbedding(
            model=embedding_model,
            base_url=ollama_base_url
        )

        # initialize vector database
        self._db = VectorDatabase(
            path=DB_PATH,
            embedding_function=self._embedding_function
        )

        # cache for collection instances
        self._collections: Dict[str, Collection] = {}

    def close(
        self,
    ) -> None:
        """
        Close the database connection.
        """

        self._db.close()
        self._collections.clear()
