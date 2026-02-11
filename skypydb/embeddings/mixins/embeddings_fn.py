"""
Module containing the EmbeddingsFn class, which is used to generate embeddings for a list of texts.
"""

from typing import (
    List,
    Optional
)

class EmbeddingsFn:
    def __init__(
        self,
        dimension: Optional[int] = None
    ) -> None:
        """
        Initialize the EmbeddingsFn with an optional dimension.

        Args:
            dimension: The dimension of the embeddings. If None, it will be inferred from the first embedding.
        """

        self._dimension = dimension

    def _get_embedding(
        self,
        text: str
    ) -> List[float]:
        """
        Get embedding for a single text.
        """

        raise NotImplementedError(
            f"{self.__class__.__name__} must implement `_get_embedding`."
        )

    def embed(
        self,
        texts: List[str]
    ) -> List[List[float]]:
        """
        Generate embeddings for a list of texts.

        Args:
            texts: List of texts to embed

        Returns:
            List of embedding vectors
        """

        embeddings: List[List[float]] = []

        for text in texts:
            embedding = self._get_embedding(text)
            embeddings.append(embedding)
            # cache the dimension from the first embedding
            if self._dimension is None:
                self._dimension = len(embedding)
        return embeddings

    def dimension(
        self
    ) -> Optional[int]:
        """
        Get the embedding dimension.

        Returns: 
            None if no embedding has been generated yet.
        """

        return self._dimension

    def get_dimension(
        self
    ) -> int:
        """
        Get embedding dimension, generating a test embedding if needed.

        Returns:
            The dimension of embeddings produced by this model.
        """

        if self._dimension is None:
            test_embedding = self._get_embedding("test")
            self._dimension = len(test_embedding)
        return self._dimension
