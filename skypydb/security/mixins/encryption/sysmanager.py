"""

"""

from typing import Optional
from skypydb.security import EncryptionManager

def create_encryption_manager(
    encryption_key: Optional[str] = None,
    salt: Optional[bytes] = None,
) -> EncryptionManager:
    """
    Factory function to create an EncryptionManager instance.

    Args:
        encryption_key: Master encryption key. If None, encryption is disabled.
        salt: Required, non-empty salt for PBKDF2HMAC when encryption is enabled.

    Returns:
        EncryptionManager instance
    """

    return EncryptionManager(encryption_key=encryption_key, salt=salt)
