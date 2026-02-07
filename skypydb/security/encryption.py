"""
Encryption module for securing sensitive data in Skypydb.
"""

from typing import Optional
from skypydb.errors import EncryptionError
from cryptography.hazmat.primitives.ciphers.aead import AESGCM
from skypydb.security.mixins.encryption import (
    SysManager,
    SysGenerator,
    SysPassword,
    SysEncrypt,
    SysDecrypt
)

class EncryptionManager(
    SysManager,
    SysGenerator,
    SysPassword,
    SysEncrypt,
    SysDecrypt
):
    """
    Manages encryption and decryption of sensitive data using AES-256-GCM.

    Features:
    - AES-256-GCM encryption (authenticated encryption)
    - PBKDF2HMAC key derivation from passwords
    - Secure random nonce generation
    - Base64 encoding for storage compatibility
    """

    def __init__(
        self,
        encryption_key: Optional[str] = None,
        iterations: int = 100000,
        salt: Optional[bytes] = None
    ):
        """
        Initialize encryption manager.

        Args:
            encryption_key: Master encryption key/password. If None, encryption is disabled.
            iterations: Number of PBKDF2HMAC iterations (default: 100000)
            salt: Required, non-empty salt for PBKDF2HMAC when encryption is enabled

        Raises:
            EncryptionError: If cryptography library is not installed
        """

        if encryption_key is not None and not encryption_key.strip():
            raise EncryptionError("encryption_key must be a non-empty string")

        self.enabled = bool(encryption_key)
        self.iterations = iterations
        self._salt = salt
        self._key: Optional[bytes] = None
        if self.enabled:
            if encryption_key == "":
                raise EncryptionError("Encryption key must not be empty.")
            # derive a 256-bit key from the password
            assert encryption_key is not None  # type narrowing for type checker
            self._key = self._derive_key(encryption_key, salt=self._salt)
            self._aesgcm = AESGCM(self._key)
