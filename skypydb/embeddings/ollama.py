"""
Ollama embedding functions for vector operations.
"""

import json
import urllib.request
import urllib.error
from typing import (
    List,
    Optional
)
from skypydb.embeddings.mixins import (
    Utils,
    EmbeddingsFn
)

class OllamaEmbedding(
    EmbeddingsFn,
    Utils
):
    def __init__(
        self,
        model: str = "mxbai-embed-large",
        base_url: str = "http://localhost:11434",
        dimension: Optional[int] = None
    ):
        """
        Initialize Ollama embedding function.

        Args:
            model: Name of the Ollama embedding model to use
            base_url: Base URL for Ollama API (default: http://localhost:11434)
        """

        super().__init__(dimension=dimension)
        self.model = model
        self.base_url = base_url.rstrip("/")

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

        base_url = getattr(self, "base_url", "http://localhost:11434").rstrip("/")

        model = getattr(self, "model", "mxbai-embed-large")

        url = f"{base_url}/api/embeddings"

        data = json.dumps({
            "model": model,
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
                        f"Make sure model '{model}' is an embedding model."
                    )
                return embedding
        except urllib.error.URLError as e:
            raise ConnectionError(
                f"Cannot connect to Ollama at {base_url}. "
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