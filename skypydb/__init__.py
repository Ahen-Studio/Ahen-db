"""
Skypydb - Open Source Reactive Database for Python.
"""

from .api.client import Client
from .api.vector_client import VectorClient
from .api.collection import Collection
from .errors import (
    DatabaseError,
    InvalidSearchError,
    SkypydbError,
    TableAlreadyExistsError,
    TableNotFoundError,
)
from .security import (
    EncryptionManager,
    EncryptionError,
    create_encryption_manager,
)
from .embeddings import (
    OllamaEmbedding,
    get_embedding_function,
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
    "get_embedding_function",
]
