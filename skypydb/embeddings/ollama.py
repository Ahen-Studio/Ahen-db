"""
Ollama embedding functions for vector operations.
"""

import json
import urllib.request
import urllib.error
from typing import List, Optional, Callable


# main class for the embedding functions using Ollama
class OllamaEmbedding:
    """
    Embedding function using Ollama models.

    Supports any embedding model available in Ollama such as:
    """


    # initialize Ollama embedding function
    def __init__(
        self,
        model: str = "mxbai-embed-large",
        base_url: str = "http://localhost:11434",
    ):
        """
        Initialize Ollama embedding function.

        Args:
            model: Name of the Ollama embedding model to use
            base_url: Base URL for Ollama API (default: http://localhost:11434)
        """

        self.model = model
        self.base_url = base_url.rstrip("/")
        self._dimension: Optional[int] = None


    # get embedding for a single text using Ollama API
    def _get_embedding(
        self,
        text: str,
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

        url = f"{self.base_url}/api/embeddings"

        data = json.dumps({
            "model": self.model,
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
                        f"Make sure model '{self.model}' is an embedding model."
                    )
                
                return embedding

        except urllib.error.URLError as e:
            raise ConnectionError(
                f"Cannot connect to Ollama at {self.base_url}. "
                f"Make sure Ollama is running. If you haven't installed it go to https://ollama.com/download and install it. Error: {e}"
            )
        except json.JSONDecodeError as e:
            raise ValueError(f"Invalid response from Ollama: {e}")


    # generate embedding for a lists of texts
    def embed(
        self,
        texts: List[str],
    ) -> List[List[float]]:
        """
        Generate embeddings for a list of texts.

        Args:
            texts: List of texts to embed

        Returns:
            List of embedding vectors
        """

        embeddings = []

        for text in texts:
            embedding = self._get_embedding(text)
            embeddings.append(embedding)

            # Cache the dimension from the first embedding
            if self._dimension is None:
                self._dimension = len(embedding)

        return embeddings


    # make the class callable for compatibility with other libraries
    def __call__(
        self,
        texts: List[str],
    ) -> List[List[float]]:
        """
        Make the class callable for compatibility with other libraries.

        Args:
            texts: List of texts to embed

        Returns:
            List of embedding vectors
        """

        return self.embed(texts)


    # get the embedding dimension
    @property
    def dimension(
        self,
    ) -> Optional[int]:
        """
        Get the embedding dimension.

        Returns None if no embedding has been generated yet.
        """

        return self._dimension


    # get the embedding dimension and generate a test embedding if needed
    def get_dimension(
        self,
    ) -> int:
        """
        Get the embedding dimension, generating a test embedding if needed.

        Returns:
            The dimension of embeddings produced by this model
        """

        if self._dimension is None:
            # Generate a test embedding to determine dimension
            test_embedding = self._get_embedding("test")
            self._dimension = len(test_embedding)

        return self._dimension


# get an embedding function using Ollama
def get_embedding_function(
    model: str = "mxbai-embed-large",
    base_url: str = "http://localhost:11434",
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

    return OllamaEmbedding(model=model, base_url=base_url)
