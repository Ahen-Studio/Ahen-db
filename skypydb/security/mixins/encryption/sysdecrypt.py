"""
Module containing the SysDecrypt class, which is used to decrypt data.
"""

import base64
from typing import Optional
from skypydb.errors import EncryptionError
from cryptography.hazmat.primitives.ciphers.aead import AESGCM
from skypydb.security.mixins.encryption import SysPassword

class SysDecrypt:
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

    def decrypt(
        self,
        encrypted_data: str
    ) -> str:
        """
        Decrypt encrypted data.

        Args:
            encrypted_data: Base64-encoded encrypted data with format: nonce|ciphertext

        Returns:
            Decrypted plaintext

        Raises:
            EncryptionError: If decryption fails
        """

        if not self.enabled:
            return encrypted_data

        try:
            # decode from base64
            encrypted_bytes = base64.b64decode(encrypted_data.encode('utf-8'))

            # extract nonce and ciphertext
            nonce = encrypted_bytes[:12]
            ciphertext = encrypted_bytes[12:]

            # decrypt the data
            plaintext = self._aesgcm.decrypt(
                nonce,
                ciphertext,
                None
            )
            return plaintext.decode('utf-8')
        except Exception as e:
            raise EncryptionError(f"Decryption failed: {str(e)}")

    def decrypt_dict(
        self,
        data: dict,
        fields_to_decrypt: Optional[list] = None
    ) -> dict:
        """
        Decrypt specific fields in a dictionary.

        Args:
            data: Dictionary containing encrypted data
            fields_to_decrypt: List of field names to decrypt. If None, decrypts all values.

        Returns:
            Dictionary with decrypted fields
        """

        if not self.enabled:
            return data

        decrypted_data = {}

        for key, value in data.items():
            if fields_to_decrypt is None or key in fields_to_decrypt:
                # decrypt this field
                if isinstance(value, str) and value:
                    try:
                        decrypted_data[key] = self.decrypt(value)
                    except EncryptionError:
                        # if decryption fails, keep original value but they might be unencrypted data
                        decrypted_data[key] = value
                else:
                    decrypted_data[key] = value
            else:
                # don't decrypt this field
                decrypted_data[key] = value
        return decrypted_data
