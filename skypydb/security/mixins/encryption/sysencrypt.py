"""
Module containing the SysEncrypt class, which is used to encrypt data.
"""

import os
import base64
from typing import Optional
from skypydb.errors import EncryptionError
from cryptography.hazmat.primitives.ciphers.aead import AESGCM
from skypydb.security.mixins.encryption import SysPassword

class SysEncrypt:
    def __init__(
        self,
        encryption_key: Optional[str] = None,
        salt: Optional[bytes] = None
    ):
        if encryption_key is not None and not encryption_key.strip():
            raise EncryptionError("encryption_key must be a non-empty string")

        self.enabled = bool(encryption_key)
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

    def encrypt(
        self,
        plaintext: str
    ) -> str:
        """
        Encrypt plaintext data.

        Args:
            plaintext: Data to encrypt

        Returns:
            Base64-encoded encrypted data with format: nonce|ciphertext

        Raises:
            EncryptionError: If encryption fails
        """

        if not self.enabled:
            return plaintext

        try:
            # generate a random 96-bit nonce
            nonce = os.urandom(12)

            # encrypt the data
            ciphertext = self._aesgcm.encrypt(
                nonce,
                plaintext.encode('utf-8'),
                None
            )

            # combine nonce and ciphertext
            encrypted_data = nonce + ciphertext

            # encode to base64 for storage
            return base64.b64encode(encrypted_data).decode('utf-8')
        except Exception as e:
            raise EncryptionError(f"Encryption failed: {str(e)}")

    def encrypt_dict(
        self,
        data: dict,
        fields_to_encrypt: Optional[list] = None
    ) -> dict:
        """
        Encrypt specific fields in a dictionary.

        Args:
            data: Dictionary containing data
            fields_to_encrypt: List of field names to encrypt. If None, encrypts all values.

        Returns:
            Dictionary with encrypted fields
        """

        if not self.enabled:
            return data

        encrypted_data = {}

        for key, value in data.items():
            if fields_to_encrypt is None or key in fields_to_encrypt:
                # encrypt this field
                if isinstance(value, str):
                    encrypted_data[key] = self.encrypt(value)
                elif value is not None:
                    # convert to string first, then encrypt
                    encrypted_data[key] = self.encrypt(str(value))
                else:
                    encrypted_data[key] = value
            else:
                # don't encrypt this field
                encrypted_data[key] = value
        return encrypted_data
