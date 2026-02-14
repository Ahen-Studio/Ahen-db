//! Security helpers for validation and encryption.

use crate::errors::{Result, SkypydbError};
use aes_gcm::aead::Aead;
use aes_gcm::{Aes256Gcm, KeyInit, Nonce};
use base64::engine::general_purpose::STANDARD as BASE64_STANDARD;
use base64::Engine;
use once_cell::sync::Lazy;
use pbkdf2::pbkdf2_hmac;
use rand::RngCore;
use regex::Regex;
use serde_json::{Map, Value};
use sha2::Sha256;

const MAX_TABLE_NAME_LENGTH: usize = 64;
const MAX_COLUMN_NAME_LENGTH: usize = 64;
const MAX_STRING_LENGTH: usize = 10_000;
const PBKDF2_ITERATIONS: u32 = 100_000;
const SQL_INJECTION_PATTERNS: &[&str] = &[
    r";\s*DROP\s+TABLE",
    r";\s*DELETE\s+FROM",
    r";\s*UPDATE\s+",
    r";\s*INSERT\s+INTO",
    r"--",
    r"/\*",
    r"\*/",
    r"xp_",
    r"sp_",
    r"EXEC\s*\(",
    r"EXECUTE\s*\(",
    r"UNION\s+SELECT",
    r"INTO\s+OUTFILE",
    r"LOAD_FILE",
];

static TABLE_NAME_PATTERN: Lazy<Regex> =
    Lazy::new(|| Regex::new(r"^[a-zA-Z_][a-zA-Z0-9_-]*$").expect("valid table name regex"));
static COLUMN_NAME_PATTERN: Lazy<Regex> =
    Lazy::new(|| Regex::new(r"^[a-zA-Z_][a-zA-Z0-9_]*$").expect("valid column name regex"));

pub struct InputValidator;

impl InputValidator {
    pub fn sanitize_string(value: &str) -> String {
        value.replace('\0', "")
    }

    fn contains_sql_injection(value: &str) -> bool {
        let value_upper = value.to_ascii_uppercase();
        SQL_INJECTION_PATTERNS.iter().any(|pattern| {
            Regex::new(pattern)
                .map(|regex| regex.is_match(&value_upper))
                .unwrap_or(false)
        })
    }

    pub fn validate_table_name(table_name: &str) -> Result<String> {
        if table_name.is_empty() {
            return Err(SkypydbError::validation("Table name cannot be empty"));
        }
        if table_name.len() > MAX_TABLE_NAME_LENGTH {
            return Err(SkypydbError::validation(format!(
                "Table name too long (max {MAX_TABLE_NAME_LENGTH} characters)"
            )));
        }
        if !TABLE_NAME_PATTERN.is_match(table_name) {
            return Err(SkypydbError::validation(
                "Table name must start with a letter or underscore and contain only alphanumeric characters, underscores, and hyphens",
            ));
        }
        if Self::contains_sql_injection(table_name) {
            return Err(SkypydbError::validation(
                "Table name contains potentially dangerous characters",
            ));
        }

        Ok(table_name.to_string())
    }

    pub fn validate_column_name(column_name: &str) -> Result<String> {
        if column_name.is_empty() {
            return Err(SkypydbError::validation("Column name cannot be empty"));
        }
        if column_name.len() > MAX_COLUMN_NAME_LENGTH {
            return Err(SkypydbError::validation(format!(
                "Column name too long (max {MAX_COLUMN_NAME_LENGTH} characters)"
            )));
        }
        if !COLUMN_NAME_PATTERN.is_match(column_name) {
            return Err(SkypydbError::validation(
                "Column name must start with a letter or underscore and contain only alphanumeric characters and underscores",
            ));
        }
        if Self::contains_sql_injection(column_name) {
            return Err(SkypydbError::validation(
                "Column name contains potentially dangerous characters",
            ));
        }

        Ok(column_name.to_string())
    }

    pub fn validate_string_value(value: &str, max_length: Option<usize>) -> Result<String> {
        let max_len = max_length.unwrap_or(MAX_STRING_LENGTH);
        if value.len() > max_len {
            return Err(SkypydbError::validation(format!(
                "String value too long (max {max_len} characters)"
            )));
        }

        Ok(value.to_string())
    }

    pub fn validate_data_dict(data: &Map<String, Value>) -> Result<Map<String, Value>> {
        let mut validated = Map::new();

        for (key, value) in data {
            let validated_key = Self::validate_column_name(key)?;
            let validated_value = match value {
                Value::String(text) => Value::String(Self::sanitize_string(text)),
                Value::Number(_) | Value::Bool(_) | Value::Null => value.clone(),
                _ => Value::String(Self::sanitize_string(&value.to_string())),
            };
            validated.insert(validated_key, validated_value);
        }

        Ok(validated)
    }

    pub fn validate_filter_map(filters: &Map<String, Value>) -> Result<Map<String, Value>> {
        let mut validated = Map::new();

        for (key, value) in filters {
            let validated_key = Self::validate_column_name(key)?;
            let validated_value = match value {
                Value::Array(values) => Value::Array(
                    values
                        .iter()
                        .map(|item| match item {
                            Value::String(text) => Value::String(Self::sanitize_string(text)),
                            Value::Number(_) | Value::Bool(_) | Value::Null => item.clone(),
                            _ => Value::String(Self::sanitize_string(&item.to_string())),
                        })
                        .collect(),
                ),
                Value::String(text) => Value::String(Self::sanitize_string(text)),
                Value::Number(_) | Value::Bool(_) | Value::Null => value.clone(),
                _ => Value::String(Self::sanitize_string(&value.to_string())),
            };
            validated.insert(validated_key, validated_value);
        }

        Ok(validated)
    }
}

#[derive(Clone, Debug)]
pub struct EncryptionManager {
    enabled: bool,
    key: Option<Vec<u8>>,
    iterations: u32,
}

impl EncryptionManager {
    pub fn new(encryption_key: Option<&str>, salt: Option<&[u8]>) -> Result<Self> {
        if let Some(key) = encryption_key {
            if key.trim().is_empty() {
                return Err(SkypydbError::encryption(
                    "encryption_key must be a non-empty string",
                ));
            }
        }

        let enabled = encryption_key.is_some();
        let derived_key = if let Some(password) = encryption_key {
            Some(Self::derive_key(password, salt, PBKDF2_ITERATIONS)?)
        } else {
            None
        };

        Ok(Self {
            enabled,
            key: derived_key,
            iterations: PBKDF2_ITERATIONS,
        })
    }

    fn derive_key(password: &str, salt: Option<&[u8]>, iterations: u32) -> Result<Vec<u8>> {
        let Some(salt) = salt else {
            return Err(SkypydbError::encryption(
                "Encryption salt must be provided and non-empty.",
            ));
        };
        if salt.is_empty() {
            return Err(SkypydbError::encryption(
                "Encryption salt must be provided and non-empty.",
            ));
        }
        let mut output = vec![0_u8; 32];
        pbkdf2_hmac::<Sha256>(password.as_bytes(), salt, iterations, &mut output);

        Ok(output)
    }

    fn cipher(&self) -> Result<Aes256Gcm> {
        let Some(key) = &self.key else {
            return Err(SkypydbError::encryption(
                "Encryption key is not initialized while encryption is enabled.",
            ));
        };
        Aes256Gcm::new_from_slice(key).map_err(|error| SkypydbError::encryption(error.to_string()))
    }

    pub fn enabled(&self) -> bool {
        self.enabled
    }

    pub fn generate_key() -> String {
        let mut bytes = [0_u8; 32];
        rand::thread_rng().fill_bytes(&mut bytes);
        hex::encode(bytes)
    }

    pub fn generate_salt(length: usize) -> Result<Vec<u8>> {
        if length == 0 {
            return Err(SkypydbError::encryption("Salt length must be positive."));
        }
        let mut salt = vec![0_u8; length];
        rand::thread_rng().fill_bytes(&mut salt);

        Ok(salt)
    }

    pub fn encrypt(&self, plaintext: &str) -> Result<String> {
        if !self.enabled {
            return Ok(plaintext.to_string());
        }
        let cipher = self.cipher()?;
        let mut nonce_bytes = [0_u8; 12];
        rand::thread_rng().fill_bytes(&mut nonce_bytes);
        let nonce = Nonce::from_slice(&nonce_bytes);
        let ciphertext = cipher
            .encrypt(nonce, plaintext.as_bytes())
            .map_err(|error| SkypydbError::encryption(format!("Encryption failed: {error}")))?;
        let mut payload = Vec::with_capacity(nonce_bytes.len() + ciphertext.len());
        payload.extend_from_slice(&nonce_bytes);
        payload.extend_from_slice(&ciphertext);

        Ok(BASE64_STANDARD.encode(payload))
    }

    pub fn decrypt(&self, encrypted_data: &str) -> Result<String> {
        if !self.enabled {
            return Ok(encrypted_data.to_string());
        }
        let payload = BASE64_STANDARD
            .decode(encrypted_data)
            .map_err(|error| SkypydbError::encryption(format!("Decryption failed: {error}")))?;
        if payload.len() < 13 {
            return Err(SkypydbError::encryption(
                "Decryption failed: invalid ciphertext payload",
            ));
        }
        let nonce = Nonce::from_slice(&payload[..12]);
        let ciphertext = &payload[12..];
        let cipher = self.cipher()?;
        let plaintext = cipher
            .decrypt(nonce, ciphertext)
            .map_err(|error| SkypydbError::encryption(format!("Decryption failed: {error}")))?;
        String::from_utf8(plaintext)
            .map_err(|error| SkypydbError::encryption(format!("Decryption failed: {error}")))
    }

    pub fn encrypt_map(
        &self,
        data: &Map<String, Value>,
        fields_to_encrypt: Option<&[String]>,
    ) -> Result<Map<String, Value>> {
        if !self.enabled {
            return Ok(data.clone());
        }
        let mut encrypted = Map::new();

        for (key, value) in data {
            let should_encrypt = match fields_to_encrypt {
                Some(fields) => fields.iter().any(|field| field == key),
                None => true,
            };
            if !should_encrypt {
                encrypted.insert(key.clone(), value.clone());
                continue;
            }
            let value_as_string = match value {
                Value::Null => {
                    encrypted.insert(key.clone(), Value::Null);
                    continue;
                }
                Value::String(text) => text.clone(),
                _ => value.to_string(),
            };

            encrypted.insert(key.clone(), Value::String(self.encrypt(&value_as_string)?));
        }

        Ok(encrypted)
    }

    pub fn decrypt_map(
        &self,
        data: &Map<String, Value>,
        fields_to_decrypt: Option<&[String]>,
    ) -> Result<Map<String, Value>> {
        if !self.enabled {
            return Ok(data.clone());
        }
        let mut decrypted = Map::new();

        for (key, value) in data {
            let should_decrypt = match fields_to_decrypt {
                Some(fields) => fields.iter().any(|field| field == key),
                None => true,
            };
            if !should_decrypt {
                decrypted.insert(key.clone(), value.clone());
                continue;
            }
            let Value::String(text) = value else {
                decrypted.insert(key.clone(), value.clone());
                continue;
            };

            match self.decrypt(text) {
                Ok(plaintext) => {
                    decrypted.insert(key.clone(), Value::String(plaintext));
                }
                Err(_) => {
                    decrypted.insert(key.clone(), value.clone());
                }
            }
        }

        Ok(decrypted)
    }

    pub fn hash_password(&self, password: &str) -> Result<String> {
        let mut salt = [0_u8; 32];
        rand::thread_rng().fill_bytes(&mut salt);
        let hash = Self::derive_key(password, Some(&salt), self.iterations)?;
        let mut payload = Vec::with_capacity(salt.len() + hash.len());
        payload.extend_from_slice(&salt);
        payload.extend_from_slice(&hash);
        Ok(BASE64_STANDARD.encode(payload))
    }

    pub fn verify_password(&self, password: &str, stored_hash: &str) -> bool {
        let Ok(payload) = BASE64_STANDARD.decode(stored_hash) else {
            return false;
        };
        if payload.len() < 64 {
            return false;
        }
        let salt = &payload[..32];
        let expected_hash = &payload[32..];
        let Ok(candidate_hash) = Self::derive_key(password, Some(salt), self.iterations) else {
            return false;
        };

        expected_hash == candidate_hash
    }
}

#[cfg(test)]
mod tests {
    use super::EncryptionManager;

    #[test]
    fn encryption_roundtrip_works() {
        let key = "test-secret";
        let salt = b"01234567890123456789012345678901";
        let manager = EncryptionManager::new(Some(key), Some(salt)).expect("manager");
        let encrypted = manager.encrypt("hello").expect("encrypt");
        assert_ne!(encrypted, "hello");
        let decrypted = manager.decrypt(&encrypted).expect("decrypt");
        assert_eq!(decrypted, "hello");
    }
}
