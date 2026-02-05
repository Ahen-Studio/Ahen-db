"""
Client API for Skypydb.
"""

import os
from typing import Optional
from skypydb.database.reactive_db import ReactiveDatabase
from skypydb.api.mixins.reactive import (
    SysCreate,
    SysGet,
    SysDelete
)

class ReactiveClient(
    SysCreate,
    SysGet,
    SysDelete
):
    """
    Reactive client for interacting with Skypydb.
    """

    def __init__(
        self,
        encryption_key: Optional[str] = None,
        salt: Optional[bytes] = None,
        encrypted_fields: Optional[list] = None
    ):
        """
        Initialize Skypydb client.

        Args:
            encryption_key: Optional encryption key for data encryption at rest.
                           If provided, sensitive data will be encrypted.
                           Generate a secure key with: EncryptionManager.generate_key()
            salt: Optional salt for PBKDF2HMAC; must be non-empty and provided
                                 when encryption is enabled (i.e., when an encryption_key is set).
            encrypted_fields: Optional list of field names to encrypt.
                             If None and encryption is enabled, all fields except
                             'id' and 'created_at' will be encrypted.

        Example:
            # Without encryption
            client = skypydb.Client()

            # With encryption (all fields encrypted by default)
            from skypydb.security import EncryptionManager

            key = EncryptionManager.generate_key()

            client = skypydb.Client(encryption_key=key)

            # With encryption (specific fields only)
            client = skypydb.Client(
                encryption_key=key,
                encrypted_fields=["content", "email", "password"]
            )
        """

        # constant to define the path to the database file
        DB_PATH = "./db/_generated/skypydb.db"

        db_dir = os.path.dirname(DB_PATH)
        if db_dir and not os.path.exists(db_dir):
            os.makedirs(db_dir, exist_ok=True)

        self.path = DB_PATH
        self.db = ReactiveDatabase(
            DB_PATH,
            encryption_key=encryption_key,
            salt=salt,
            encrypted_fields=encrypted_fields
        )

    def close(self) -> None:
        """
        Close database connection.
        """

        self.db.close()
