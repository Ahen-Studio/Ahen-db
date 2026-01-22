"""
Encryption module for securing sensitive data in SkypyDB.
Uses AES-256-GCM for encryption with PBKDF2 key derivation.
"""

import base64
import os
import secrets
from typing import Optional
from ..errors import EncryptionError
from cryptography.hazmat.primitives.ciphers.aead import AESGCM
from cryptography.hazmat.primitives import hashes
from cryptography.hazmat.primitives.kdf.pbkdf2 import PBKDF2HMAC
from cryptography.hazmat.backends import default_backend


class EncryptionManager:
    """
    Manages encryption and decryption of sensitive data using AES-256-GCM.
    
    Features:
    - AES-256-GCM encryption (authenticated encryption)
    - PBKDF2 key derivation from passwords
    - Secure random nonce generation
    - Base64 encoding for storage compatibility
    """

    def __init__(
        self,
        encryption_key: Optional[str] = None,
        iterations: int = 100000,
    ):
        """
        Initialize encryption manager.

        Args:
            encryption_key: Master encryption key/password. If None, encryption is disabled.
            iterations: Number of PBKDF2 iterations (default: 100000)

        Raises:
            EncryptionError: If cryptography library is not installed
        """

        self.enabled = encryption_key is not None
        self.iterations = iterations
        self._key: Optional[bytes] = None

        if self.enabled:
            # Derive a 256-bit key from the password
            assert encryption_key is not None  # Type narrowing for type checker
            self._key = self._derive_key(encryption_key)
            self._aesgcm = AESGCM(self._key)
    
    def _derive_key(
        self,
        password: str,
        salt: Optional[bytes] = None,
    ) -> bytes:
        """
        Derive a 256-bit encryption key from a password using PBKDF2.
        
        Args:
            password: Master password/key
            salt: Optional salt (if None, uses a fixed application salt)
            
        Returns:
            32-byte encryption key
        """
        
        # Use a fixed application salt for key derivation
        # need to be generated per-database, next pr will address this
        if salt is None:
            salt = b"skypydb_encryption_salt_v1"
        
        kdf = PBKDF2HMAC(
            algorithm=hashes.SHA256(),
            length=32,  # 256 bits
            salt=salt,
            iterations=self.iterations,
            backend=default_backend()
        )
        
        return kdf.derive(password.encode('utf-8'))
    
    def encrypt(
        self,
        plaintext: str,
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
            # Generate a random 96-bit nonce (12 bytes - recommended for GCM)
            nonce = os.urandom(12)
            
            # Encrypt the data
            ciphertext = self._aesgcm.encrypt(
                nonce,
                plaintext.encode('utf-8'),
                None  # No additional authenticated data
            )
            
            # Combine nonce and ciphertext
            encrypted_data = nonce + ciphertext
            
            # Encode to base64 for storage
            return base64.b64encode(encrypted_data).decode('utf-8')
            
        except Exception as e:
            raise EncryptionError(f"Encryption failed: {str(e)}")
    
    def decrypt(
        self,
        encrypted_data: str,
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
            # Decode from base64
            encrypted_bytes = base64.b64decode(encrypted_data.encode('utf-8'))
            
            # Extract nonce and ciphertext
            nonce = encrypted_bytes[:12]
            ciphertext = encrypted_bytes[12:]
            
            # Decrypt the data
            plaintext = self._aesgcm.decrypt(
                nonce,
                ciphertext,
                None  # No additional authenticated data
            )
            
            return plaintext.decode('utf-8')
            
        except Exception as e:
            raise EncryptionError(f"Decryption failed: {str(e)}")
    
    def encrypt_dict(
        self,
        data: dict,
        fields_to_encrypt: Optional[list] = None,
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
                # Encrypt this field
                if isinstance(value, str):
                    encrypted_data[key] = self.encrypt(value)
                elif value is not None:
                    # Convert to string first, then encrypt
                    encrypted_data[key] = self.encrypt(str(value))
                else:
                    encrypted_data[key] = value
            else:
                # Don't encrypt this field
                encrypted_data[key] = value
        
        return encrypted_data
    
    def decrypt_dict(
        self,
        data: dict,
        fields_to_decrypt: Optional[list] = None,
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
                # Decrypt this field
                if isinstance(value, str) and value:
                    try:
                        decrypted_data[key] = self.decrypt(value)
                    except EncryptionError:
                        # If decryption fails, keep original value
                        # (might be unencrypted data)
                        decrypted_data[key] = value
                else:
                    decrypted_data[key] = value
            else:
                # Don't decrypt this field
                decrypted_data[key] = value
        
        return decrypted_data
    
    def hash_password(
        self,
        password: str,
    ) -> str:
        """
        Create a secure hash of a password for storage.
        Uses PBKDF2HMAC with a random salt.
        
        Args:
            password: Password to hash
            
        Returns:
            Base64-encoded hash with format: salt|hash
        """
        
        # Generate a random salt
        salt = secrets.token_bytes(32)
        
        # Hash the password
        kdf = PBKDF2HMAC(
            algorithm=hashes.SHA256(),
            length=32,
            salt=salt,
            iterations=self.iterations,
            backend=default_backend()
        )
        
        password_hash = kdf.derive(password.encode('utf-8'))
        
        # Combine salt and hash
        combined = salt + password_hash
        
        return base64.b64encode(combined).decode('utf-8')
    
    def verify_password(
        self,
        password: str,
        stored_hash: str,
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
            # Decode the stored hash
            combined = base64.b64decode(stored_hash.encode('utf-8'))
            
            # Extract salt and hash
            salt = combined[:32]
            stored_password_hash = combined[32:]
            
            # Hash the provided password with the same salt
            kdf = PBKDF2HMAC(
                algorithm=hashes.SHA256(),
                length=32,
                salt=salt,
                iterations=self.iterations,
                backend=default_backend()
            )
            
            password_hash = kdf.derive(password.encode('utf-8'))
            
            # Compare hashes using constant-time comparison
            return secrets.compare_digest(password_hash, stored_password_hash)
            
        except Exception:
            return False
    
    @staticmethod
    def generate_key() -> str:
        """
        Generate a secure random encryption key.
        
        Returns:
            Random 256-bit key encoded as hex string
        """
        
        return secrets.token_hex(32)  # 32 bytes = 256 bits


def create_encryption_manager(
    encryption_key: Optional[str] = None,
) -> EncryptionManager:
    """
    Factory function to create an EncryptionManager instance.
    
    Args:
        encryption_key: Master encryption key. If None, encryption is disabled.
        
    Returns:
        EncryptionManager instance
    """
    
    return EncryptionManager(encryption_key=encryption_key)
