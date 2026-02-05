"""
Module containing the SysGet class, which is used to get embedding in ollama.
"""

import json
import urllib.request
import urllib.error
from typing import (
    List,
    Callable
)

class SysGet:
    def _get_embedding(
        self,
        text: str
    ) -> List[float]:
        """
        Get embedding for a single text using Ollama API.

        Args:
            text: Text to embed

        Returns:
            List of floats representing the embedding vector

        Raises:
            ConnectionError: If Ollama server is not reachable
            ValueError: If embedding generation fails
        """

        from skypydb.embeddings.ollama import OllamaEmbedding
        self.embedder = OllamaEmbedding()

        url = f"{self.embedder.base_url}/api/embeddings"

        data = json.dumps({
            "model": self.embedder.model,
            "prompt": text
        }).encode("utf-8")

        request = urllib.request.Request(
            url,
            data=data,
            headers={"Content-Type": "application/json"},
            method="POST"
        )

        try:
            with urllib.request.urlopen(request, timeout=60) as response:
                result = json.loads(response.read().decode("utf-8"))
                embedding = result.get("embedding")

                if embedding is None:
                    raise ValueError(
                        f"No embedding returned from Ollama. "
                        f"Make sure model '{self.embedder.model}' is an embedding model."
                    )
                return embedding

        except urllib.error.URLError as e:
            raise ConnectionError(
                f"Cannot connect to Ollama at {self.embedder.base_url}. "
                f"Make sure Ollama is running. If you haven't installed it go to https://ollama.com/download and install it. Error: {e}"
            )
        except json.JSONDecodeError as e:
            raise ValueError(f"Invalid response from Ollama: {e}")

    def get_dimension(
        self
    ) -> int:
        """
        Get the embedding dimension, generating a test embedding if needed.

        Returns:
            The dimension of embeddings produced by this model
        """

        if self._dimension is None:
            # generate a test embedding to determine dimension
            test_embedding = self._get_embedding("test")
            self._dimension = len(test_embedding)

        return self._dimension

def get_embedding_function(
    model: str = "mxbai-embed-large",
    base_url: str = "http://localhost:11434"
) -> Callable[[List[str]], List[List[float]]]:
    """
    Get an embedding function using Ollama.

    This is a convenience function that returns a callable
    embedding function for use with the vector database.

    Args:
        model: Name of the Ollama embedding model
        base_url: Base URL for Ollama API

    Returns:
        Callable that takes a list of texts and returns embeddings

    Example:
        embed_fn = get_embedding_function(model="mxbai-embed-large")
        embeddings = embed_fn(["Hello world", "How are you?"])
    """

    from skypydb.embeddings.ollama import OllamaEmbedding
    return OllamaEmbedding(model=model, base_url=base_url)
