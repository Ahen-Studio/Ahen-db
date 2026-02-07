"""
Module containing the base definition, which are used to calculate cosine and euclidean vectors.
"""

import math
from typing import List

def cosine_similarity(
    vec1: List[float],
    vec2: List[float]
) -> float:
    """
    Calculate cosine similarity between two vectors.

    Args:
        vec1: First vector
        vec2: Second vector

    Returns:
        Cosine similarity score between -1 and 1
    """

    if len(vec1) != len(vec2):
        raise ValueError(f"Vector dimensions don't match: {len(vec1)} vs {len(vec2)}")

    dot_product = sum(a * b for a, b in zip(vec1, vec2))
    norm1 = math.sqrt(sum(a * a for a in vec1))
    norm2 = math.sqrt(sum(b * b for b in vec2))
    if norm1 == 0 or norm2 == 0:
        return 0.0
    return dot_product / (norm1 * norm2)

def euclidean_distance(
    vec1: List[float],
    vec2: List[float]
) -> float:
    """
    Calculate Euclidean distance between two vectors.

    Args:
        vec1: First vector
        vec2: Second vector

    Returns:
        Euclidean distance (lower is more similar)
    """

    if len(vec1) != len(vec2):
        raise ValueError(f"Vector dimensions don't match: {len(vec1)} vs {len(vec2)}")
    return math.sqrt(sum((a - b) ** 2 for a, b in zip(vec1, vec2)))