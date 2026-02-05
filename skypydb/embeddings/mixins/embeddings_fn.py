"""
Module containing the EmbeddingsFn class, which is used to generate embeddings for a list of texts.
"""

from typing import (
    List,
    Optional
)
from skypydb.embeddings.mixins.sysget import SysGet

class EmbeddingsFn:
    def __init__(self) -> None:
        self._dimension: Optional[int] = None

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

        self.sysget = SysGet()

        embeddings = []

        for text in texts:
            embedding = self.sysget._get_embedding(text)
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
