"""
Custom exceptions for Skypydb.
"""

# base errors handling
class SkypydbError(Exception):
    """
    Base exception for all Skypydb errors.
    """

    code = "SKY001"

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
            formatted_message = f"[{self.code}] {self.message}"
        else:
            formatted_message = f"[{self.code}] {self.__class__.__name__}"

        super().__init__(formatted_message)


# table not found error handling
class TableNotFoundError(SkypydbError):
    """
    Raised when a table is not found.
    """

    code = "SKY101"


# table already exists error handling
class TableAlreadyExistsError(SkypydbError):
    """
    Raised when trying to create a table that already exists.
    """

    code = "SKY102"


# database errors handling
class DatabaseError(SkypydbError):
    """
    Raised when a database operation fails.
    """

    code = "SKY103"


# search errors handling
class InvalidSearchError(SkypydbError):
    """
    Raised when search parameters are invalid.
    """

    code = "SKY201"


# security errors handling
class SecurityError(SkypydbError):
    """
    Raised when a security operation fails.
    """

    code = "SKY301"


# validation errors handling
class ValidationError(SkypydbError):
    """
    Raised when input validation fails.
    """

    code = "SKY302"


# encryption errors handling
class EncryptionError(SkypydbError):
    """
    Raised when encryption/decryption operations fail.
    """

    code = "SKY303"


# collection not found error handling
class CollectionNotFoundError(SkypydbError):
    """
    Raised when a vector collection is not found.
    """

    code = "SKY401"


# collection already exists error handling
class CollectionAlreadyExistsError(SkypydbError):
    """
    Raised when trying to create a collection that already exists.
    """

    code = "SKY402"


# embedding errors handling
class EmbeddingError(SkypydbError):
    """
    Raised when embedding generation fails.
    """

    code = "SKY403"


# vector search errors handling
class VectorSearchError(SkypydbError):
    """
    Raised when vector similarity search fails.
    """

    code = "SKY404"
