"""
Custom exceptions for Skypydb.
"""

# base errors handling
class SkypydbError(Exception):
    """
    Base exception for all Skypydb errors.
    """

    pass


# database errors handling
class TableNotFoundError(SkypydbError):
    """
    Raised when a table is not found.
    """

    pass


# table errors handling
class TableAlreadyExistsError(SkypydbError):
    """
    Raised when trying to create a table that already exists.
    """

    pass


# database errors handling
class DatabaseError(SkypydbError):
    """
    Raised when a database operation fails.
    """

    pass


# search errors handling
class InvalidSearchError(SkypydbError):
    """
    Raised when search parameters are invalid.
    """

    pass


# security errors handling
class SecurityError(SkypydbError):
    """
    Raised when a security operation fails.
    """

    pass


# validation errors handling
class ValidationError(SkypydbError):
    """
    Raised when input validation fails.
    """

    pass


# encryption errors handling
class EncryptionError(SkypydbError):
    """
    Raised when encryption/decryption operations fail.
    """

    pass


# collection errors handling
class CollectionNotFoundError(SkypydbError):
    """
    Raised when a vector collection is not found.
    """

    pass


# collection errors handling
class CollectionAlreadyExistsError(SkypydbError):
    """
    Raised when trying to create a collection that already exists.
    """

    pass


# embedding errors handling
class EmbeddingError(SkypydbError):
    """
    Raised when embedding generation fails.
    """

    pass


# vector search errors handling
class VectorSearchError(SkypydbError):
    """
    Raised when vector similarity search fails.
    """

    pass
