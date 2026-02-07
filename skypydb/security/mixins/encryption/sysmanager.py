"""
Module containing the SysManager class, which is used to create an EncryptionManager instance.
"""

from typing import Optional
from skypydb.errors import EncryptionError
from cryptography.hazmat.primitives.ciphers.aead import AESGCM
from skypydb.security.mixins.encryption.syspassword import SysPassword

class SysManager:
    def __init__(
        self,
        encryption_key: Optional[str] = None,
        iterations: int = 100000,
        salt: Optional[bytes] = None
    ):
        if encryption_key is not None and not encryption_key.strip():
            raise EncryptionError("encryption_key must be a non-empty string")

        self.enabled = bool(encryption_key)
        self.iterations = iterations
        self._salt = salt
        self._key: Optional[bytes] = None
        self._password = SysPassword()
        if self.enabled:
            if encryption_key == "":
                raise EncryptionError("Encryption key must not be empty.")
            # derive a 256-bit key from the password
            assert encryption_key is not None  # type narrowing for type checker
            self._key = self._password._derive_key(encryption_key, salt=self._salt)
            self._aesgcm = AESGCM(self._key)

def create_encryption_manager(
    encryption_key: Optional[str] = None,
    salt: Optional[bytes] = None,
) -> SysManager:
    """
    Factory function to create an EncryptionManager instance.

    Args:
        encryption_key: Master encryption key. If None, encryption is disabled.
        salt: Required, non-empty salt for PBKDF2HMAC when encryption is enabled.

    Returns:
        EncryptionManager instance
    """

    return SysManager(encryption_key=encryption_key, salt=salt)
