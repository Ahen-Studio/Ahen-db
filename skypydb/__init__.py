"""
SkypyDB - Open-source reactive database for Python.
"""

from .api.client import Client
from .errors import (
    DatabaseError,
    InvalidSearchError,
    SkypyDBError,
    TableAlreadyExistsError,
    TableNotFoundError,
)

__version__ = "0.1.0"
__all__ = [
    "Client",
    "SkypyDBError",
    "DatabaseError",
    "TableNotFoundError",
    "TableAlreadyExistsError",
    "InvalidSearchError",
]
