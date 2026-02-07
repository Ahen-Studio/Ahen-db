"""
Module containing the Encryption class, which is used to encrypt and decrypt data.
"""

from typing import (
    Dict,
    Any,
    Optional
)
from skypydb.security.encryption import EncryptionManager

class Encryption:
    def __init__(
        self,
        path: str,
        encryption_key: Optional[str] = None,
        salt: Optional[bytes] = None,
        encrypted_fields: Optional[list] = None
    ):
        """
        Initialize the encryption module.

        Args:
            encryption_key: Optional encryption key for data encryption
            salt: Optional salt for encryption key derivation
            encrypted_fields: Optional list of fields to encrypt
        """

        self.encryption_key = encryption_key
        self.salt = salt
        if encryption_key and encrypted_fields is None:
            raise ValueError(
                "encrypted_fields must be explicitly set when encryption_key is provided; "
                "use [] to disable encryption."
            )
        self.encrypted_fields = encrypted_fields if encrypted_fields is not None else []

        # initialize encryption manager
        self._encryption_manager = EncryptionManager(encryption_key=encryption_key, salt=salt)

    def encrypt_data(
        self,
        data: Dict[str, Any]
    ) -> Dict[str, Any]:
        """
        Encrypt sensitive fields in data dictionary.

        Args:
            data: Dictionary containing data to encrypt

        Returns:
            Dictionary with encrypted fields
        """

        if not self._encryption_manager.enabled:
            return data

        # determine which fields to encrypt
        fields_to_encrypt = [
            key for key in data.keys() 
            if key in self.encrypted_fields
        ]
        return self._encryption_manager.encrypt_dict(data, fields_to_encrypt)

    def decrypt_data(
        self,
        data: Dict[str, Any]
    ) -> Dict[str, Any]:
        """
        Decrypt sensitive fields in data dictionary.

        Args:
            data: Dictionary containing encrypted data

        Returns:
            Dictionary with decrypted fields
        """

        if not self._encryption_manager.enabled:
            return data

        # determine which fields to decrypt
        fields_to_decrypt = [
            key for key in data.keys() 
            if key in self.encrypted_fields
        ]
        return self._encryption_manager.decrypt_dict(data, fields_to_decrypt)
