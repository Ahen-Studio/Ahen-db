"""
OpenAI embedding functions for vector operations.
"""

import os
from typing import (
    List,
    Optional,
    Any
)
from skypydb.embeddings.mixins import (
    EmbeddingsFn,
    Utils
)

class OpenAIEmbedding(
    EmbeddingsFn,
    Utils
):
    def __init__(
        self,
        api_key: Optional[str] = None,
        model: str = "text-embedding-3-small",
        base_url: Optional[str] = None,
        organization: Optional[str] = None,
        project: Optional[str] = None,
        timeout: Optional[float] = None,
        dimension: Optional[int] = None
    ):
        """
        Initialize OpenAI embedding function.

        Args:
            api_key: OpenAI API key. If not provided, OPENAI_API_KEY is used.
            model: OpenAI embedding model.
            base_url: Optional custom OpenAI-compatible base URL.
            organization: Optional OpenAI organization ID.
            project: Optional OpenAI project ID.
            timeout: Optional timeout in seconds.
        """

        super().__init__(dimension=dimension)
        self.api_key = api_key or os.getenv("OPENAI_API_KEY")
        if not self.api_key:
            raise ValueError(
                "OpenAI API key is required. Provide `api_key` or set OPENAI_API_KEY."
            )

        self.model = model
        self.base_url = base_url
        self.organization = organization
        self.project = project
        self.timeout = timeout

        try:
            from openai import OpenAI
        except ImportError as exc:
            raise ImportError(
                "OpenAI embedding provider requires the `openai` package. "
                "Install it with `pip install openai`."
            ) from exc

        client_kwargs: dict[str, Any] = {
            "api_key": self.api_key
        }
        if self.base_url is not None:
            client_kwargs["base_url"] = self.base_url
        if self.organization is not None:
            client_kwargs["organization"] = self.organization
        if self.project is not None:
            client_kwargs["project"] = self.project
        if self.timeout is not None:
            client_kwargs["timeout"] = self.timeout

        self._client = OpenAI(**client_kwargs)

    def _get_embedding(
        self,
        text: str
    ) -> List[float]:
        response = self._client.embeddings.create(
            model=self.model,
            input=[text]
        )
        return list(response.data[0].embedding)

    def embed(
        self,
        texts: List[str]
    ) -> List[List[float]]:
        """
        Generate embeddings for a list of texts using OpenAI API.
        """

        if not texts:
            return []

        response = self._client.embeddings.create(
            model=self.model,
            input=texts
        )
        embeddings = [list(item.embedding) for item in response.data]
        if self._dimension is None and embeddings:
            self._dimension = len(embeddings[0])
        return embeddings
