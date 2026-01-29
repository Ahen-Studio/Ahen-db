"""
Embeddings module for vector operations.
"""

from .ollama import (
    OllamaEmbedding,
    get_embedding_function,
)


__all__ = [
    "OllamaEmbedding",
    "get_embedding_function"
]
