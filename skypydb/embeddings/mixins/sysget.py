"""
Embedding provider factory helpers.
"""

from typing import (
    Any,
    Callable,
    List
)

def _validate_remaining_config(
    provider: str,
    config: dict
) -> None:
    """
    Validate that no unsupported config keys are provided.
    """

    if config:
        unsupported_keys = ", ".join(sorted(config.keys()))
        raise ValueError(
            f"Unsupported embedding config keys for provider '{provider}': {unsupported_keys}"
        )

def get_embedding_function(
    provider: str = "ollama",
    **config: Any
) -> Callable[[List[str]], List[List[float]]]:
    """
    Get an embedding function from supported providers.

    Args:
        provider: Embedding provider (ollama, openai, sentence-transformers)
        **config: Provider-specific configuration
    """

    provider = provider.lower().strip().replace("_", "-")

    # ollama provider
    if provider == "ollama":
        from skypydb.embeddings.ollama import OllamaEmbedding

        model = config.pop("model", "mxbai-embed-large")
        base_url = config.pop("base_url", "http://localhost:11434")
        dimension = config.pop("dimension", None)
        _validate_remaining_config(provider, config)
        return OllamaEmbedding(model=model, base_url=base_url, dimension=dimension)

    # openai provider
    if provider == "openai":
        from skypydb.embeddings.openai import OpenAIEmbedding

        api_key = config.pop("api_key", None)
        model = config.pop("model", "text-embedding-3-small")
        base_url = config.pop("base_url", None)
        organization = config.pop("organization", None)
        project = config.pop("project", None)
        timeout = config.pop("timeout", None)
        dimension = config.pop("dimension", None)
        _validate_remaining_config(provider, config)
        return OpenAIEmbedding(
            api_key=api_key,
            model=model,
            base_url=base_url,
            organization=organization,
            project=project,
            timeout=timeout,
            dimension=dimension
        )

    # sentence-transformers provider
    if provider in {"sentence-transformers", "sentence-transformer"}:
        from skypydb.embeddings.sentence_transformers import SentenceTransformerEmbedding

        model = config.pop("model", "all-MiniLM-L6-v2")
        device = config.pop("device", None)
        normalize_embeddings = config.pop("normalize_embeddings", False)
        dimension = config.pop("dimension", None)
        _validate_remaining_config(provider, config)
        return SentenceTransformerEmbedding(
            model=model,
            device=device,
            normalize_embeddings=normalize_embeddings,
            dimension=dimension
        )
    raise ValueError(
        f"Unsupported embedding provider '{provider}'. "
        "Supported providers: ollama, openai, sentence-transformers."
    )