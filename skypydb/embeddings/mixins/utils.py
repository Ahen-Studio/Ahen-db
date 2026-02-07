"""
Module containing the Utils class, which is used to make the embed class callable.
"""

from typing import List
from skypydb.embeddings.mixins import EmbeddingsFn

class Utils:
    def __call__(
        self,
        texts: List[str]
    ) -> List[List[float]]:
        """
        Make the class callable for compatibility with other libraries.

        Args:
            texts: List of texts to embed

        Returns:
            List of embedding vectors
        """
        
        self.embeddingfn = EmbeddingsFn()
        return self.embeddingfn.embed(texts)
