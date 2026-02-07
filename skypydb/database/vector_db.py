"""
Vector database backend using SQLite for storing and querying embeddings.
"""

import sqlite3
from pathlib import Path
from typing import (
    List,
    Optional,
    Callable
)
from skypydb.database.mixins.vector import (
    SysEmbeddings,
    SysAdd,
    SysUpdate,
    SysQuery,
    VSysGet,
    VSysDelete,
    AuditCollections,
    SysCreate,
    SysGet,
    SysCount,
    SysDelete
)

class VectorDatabase(
    SysEmbeddings,
    SysAdd,
    SysUpdate,
    SysQuery,
    VSysGet,
    VSysDelete,
    AuditCollections,
    SysCreate,
    SysGet,
    SysCount,
    SysDelete
):
    """
    Manages SQLite database for vector storage and similarity search.
    """

    def __init__(
        self,
        path: str,
        embedding_function: Optional[Callable[[List[str]], List[List[float]]]] = None
    ):
        """
        Initialize vector database.

        Args:
            path: Path to SQLite database file
            embedding_function: Optional function to generate embeddings from text
        """

        self.path = path
        self.embedding_function = embedding_function

        # create directory if it doesn't exist
        Path(path).parent.mkdir(parents=True, exist_ok=True)

        # connect to SQLite database
        self.conn = sqlite3.connect(path, check_same_thread=False)
        self.conn.row_factory = sqlite3.Row

        # create collections metadata table
        self._ensure_collections_table()

    def close(self) -> None:
        """
        Close database connection.
        """

        if self.conn:
            self.conn.close()
