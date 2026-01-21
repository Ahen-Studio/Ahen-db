"""
Custom exceptions for SkypyDB.
"""


class SkypyDBError(Exception):
    """
    Base exception for all SkypyDB errors.
    """

    pass


class TableNotFoundError(SkypyDBError):
    """
    Raised when a table is not found.
    """

    pass


class TableAlreadyExistsError(SkypyDBError):
    """
    Raised when trying to create a table that already exists.
    """

    pass


class DatabaseError(SkypyDBError):
    """
    Raised when a database operation fails.
    """

    pass


class InvalidSearchError(SkypyDBError):
    """
    Raised when search parameters are invalid.
    """

    pass
