"""
Skypydb - Open Source Reactive and Vector Embedding Database for Python.
"""

from skypydb.api.client import Client
from skypydb.api.vector_client import VectorClient
from skypydb.api.collection import Collection
from skypydb.errors import (
    DatabaseError,
    InvalidSearchError,
    SkypydbError,
    TableAlreadyExistsError,
    TableNotFoundError
)
from skypydb.security import (
    EncryptionManager,
    EncryptionError,
    create_encryption_manager
)
from skypydb.embeddings import (
    OllamaEmbedding,
    get_embedding_function
)

__version__ = "0.1.10"

__all__ = [
    "Client",
    "VectorClient",
    "Collection",
    "SkypydbError",
    "DatabaseError",
    "TableNotFoundError",
    "TableAlreadyExistsError",
    "InvalidSearchError",
    "EncryptionManager",
    "EncryptionError",
    "create_encryption_manager",
    "OllamaEmbedding",
    "get_embedding_function"
]
