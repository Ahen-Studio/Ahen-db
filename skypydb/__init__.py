"""
Skypydb - Open Source Reactive and Vector Embedding Database for Python.
"""

from skypydb.api.reactive_client import ReactiveClient
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
    OpenAIEmbedding,
    SentenceTransformerEmbedding,
    get_embedding_function
)

__version__ = "1.0.0"

__all__ = [
    "ReactiveClient",
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
    "OpenAIEmbedding",
    "SentenceTransformerEmbedding",
    "get_embedding_function"
]
