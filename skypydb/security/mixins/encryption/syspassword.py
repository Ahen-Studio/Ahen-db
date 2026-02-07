"""
Module containing the SysPassword class, which is used to securely manage passwords.
"""

import base64
import secrets
from typing import Optional
from skypydb.errors import EncryptionError
from cryptography.hazmat.primitives import hashes
from cryptography.hazmat.primitives.kdf.pbkdf2 import PBKDF2HMAC
from cryptography.hazmat.backends import default_backend

class SysPassword:
    def __init__(
        self,
        iterations: int = 100000
    ):
        self.iterations = iterations

    def _derive_key(
        self,
        password: str,
        salt: Optional[bytes] = None
    ) -> bytes:
        """
        Derive a 256-bit encryption key from a password using PBKDF2HMAC.

        Args:
            password: Master password/key
            salt: Required, non-empty salt for PBKDF2HMAC

        Returns:
            32-byte encryption key
        """

        if not salt:
            raise EncryptionError("Encryption salt must be provided and non-empty.")

        kdf = PBKDF2HMAC(
            algorithm=hashes.SHA256(),
            length=32,  # 256 bits
            salt=salt,
            iterations=self.iterations,
            backend=default_backend()
        )
        return kdf.derive(password.encode('utf-8'))

    def hash_password(
        self,
        password: str
    ) -> str:
        """
        Create a secure hash of a password for storage using PBKDF2HMAC with a random salt.

        Args:
            password: Password to hash

        Returns:
            Base64-encoded hash with format: salt|hash
        """

        # generate a random salt
        salt = secrets.token_bytes(32)

        # hash the password
        kdf = PBKDF2HMAC(
            algorithm=hashes.SHA256(),
            length=32,
            salt=salt,
            iterations=self.iterations,
            backend=default_backend()
        )

        password_hash = kdf.derive(password.encode('utf-8'))

        # combine salt and hash
        combined = salt + password_hash
        return base64.b64encode(combined).decode('utf-8')

    def verify_password(
        self,
        password: str,
        stored_hash: str
    ) -> bool:
        """
        Verify a password against a stored hash.

        Args:
            password: Password to verify
            stored_hash: Stored hash from hash_password()

        Returns:
            True if password matches, False otherwise
        """

        try:
            # decode the stored hash
            combined = base64.b64decode(stored_hash.encode('utf-8'))

            # extract salt and hash
            salt = combined[:32]
            stored_password_hash = combined[32:]

            # hash the provided password with the same salt
            kdf = PBKDF2HMAC(
                algorithm=hashes.SHA256(),
                length=32,
                salt=salt,
                iterations=self.iterations,
                backend=default_backend()
            )

            password_hash = kdf.derive(password.encode('utf-8'))

            # compare hashes using constant-time comparison
            return secrets.compare_digest(password_hash, stored_password_hash)
        except Exception:
            return False
