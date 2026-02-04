"""
Module containing the SysGenerator class, which is used to generate secure random encryption key and salt.
"""

import secrets
from skypydb.errors import EncryptionError

class SysGenerator:
    @staticmethod
    def generate_key() -> str:
        """
        Generate a secure random encryption key.

        Returns:
            Random 256-bit key encoded as hex string
        """

        return secrets.token_hex(32)  # 32 bytes = 256 bits

    @staticmethod
    def generate_salt(length: int = 32) -> bytes:
        """
        Generate a secure random salt.

        Args:
            length: Salt length in bytes (default: 32)

        Returns:
            Random salt as bytes
        """

        if length <= 0:
            raise EncryptionError("Salt length must be positive.")
        return secrets.token_bytes(length)
