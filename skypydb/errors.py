"""
Custom exceptions for Skypydb.
"""

# base error handling
class SkypydbError(Exception):
    """
    Base exception for all Skypydb errors.
    """

    CODE = "SKY001"
    default_message = "An unexpected error occurred in Skypydb."

    # initialize the SkypydbError instance for handling and formatting error messages
    def __init__(
        self,
        message=None,
    ):
        """
        Initialize the SkypydbError instance.

        Args:
            message (str, optional): The error message. Defaults to None.
        """

        self.message = message

        if self.message:
            formatted_message = f"[{self.CODE}] {self.message}"
        else:
            # Use a class-specific default message when provided, otherwise fall back
            # to a generic, user-friendly message when no default_message is defined.
            default_msg = getattr(
                self,
                "default_message",
                "An unexpected error occurred.",
            )
            formatted_message = f"[{self.CODE}] {default_msg}"

        super().__init__(formatted_message)


# table not found error handling
class TableNotFoundError(SkypydbError):
    """
    Raised when a table is not found.
    """

    CODE = "SKY101"
    default_message = "Table not found."


# table already exists error handling
class TableAlreadyExistsError(SkypydbError):
    """
    Raised when trying to create a table that already exists.
    """

    CODE = "SKY102"
    default_message = "Table already exists."


# database error handling
class DatabaseError(SkypydbError):
    """
    Raised when a database-level operation fails.
    """

    CODE = "SKY103"
    default_message = (
        "Database operation failed. This may indicate a connectivity issue, "
        "invalid query, or transaction problem. Check database logs and configuration."
    )


# search error handling
class InvalidSearchError(SkypydbError):
    """
    Raised when search parameters are invalid.
    """

    CODE = "SKY201"
    default_message = (
        "One or more search parameters are invalid. "
        "Check parameter names, types, and value ranges for your search query."
    )


# security errors handling
class SecurityError(SkypydbError):
    """
    Raised when a security operation fails.
    """

    CODE = "SKY301"
    default_message = (
        "Security operation failed. Possible authentication or authorization issue; "
        "see logs for details."
    )


# validation errors handling
class ValidationError(SkypydbError):
    """
    Raised when input validation fails.
    """

    CODE = "SKY302"
    default_message = "Input validation failed."


# encryption errors handling
class EncryptionError(SkypydbError):
    """
    Raised when encryption/decryption operations fail.
    """

    CODE = "SKY303"
    default_message = "Encryption or decryption operation failed."


# collection not found error handling
class CollectionNotFoundError(SkypydbError):
    """
    Raised when a vector collection is not found.
    """

    CODE = "SKY401"
    default_message = "Collection not found."


# collection already exists error handling
class CollectionAlreadyExistsError(SkypydbError):
    """
    Raised when trying to create a collection that already exists.
    """

    CODE = "SKY402"
    default_message = "Collection already exists."


# embedding errors handling
class EmbeddingError(SkypydbError):
    """
    Raised when embedding generation fails.
    """

    CODE = "SKY403"
    default_message = "Embedding generation failed."


# vector search errors handling
class VectorSearchError(SkypydbError):
    """
    Raised when vector similarity search fails.
    """

    CODE = "SKY404"
    default_message = "Vector similarity search failed."
