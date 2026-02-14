//! Database linker metadata helpers.

use crate::errors::{Result, SkypydbError};
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::path::{Path, PathBuf};
use walkdir::WalkDir;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum DatabaseType {
    Reactive,
    Vector,
}

impl DatabaseType {
    fn as_str(self) -> &'static str {
        match self {
            DatabaseType::Reactive => "reactive",
            DatabaseType::Vector => "vector",
        }
    }

    fn type_code(self) -> u8 {
        match self {
            DatabaseType::Reactive => 1,
            DatabaseType::Vector => 2,
        }
    }

    fn from_type_code(value: u8) -> Option<Self> {
        match value {
            1 => Some(DatabaseType::Reactive),
            2 => Some(DatabaseType::Vector),
            _ => None,
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DbLink {
    pub db_type: String,
    pub path: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DiscoveredDbLink {
    pub db_type: String,
    pub path: String,
    pub metadata_file: String,
}

#[derive(Debug, Clone)]
pub struct DatabaseLinker {
    folder: String,
    binary_file_type: HashMap<String, String>,
    magic: Vec<u8>,
}

impl Default for DatabaseLinker {
    fn default() -> Self {
        let mut binary_file_type = HashMap::new();
        binary_file_type.insert("reactive".to_string(), "reactivetype.bin".to_string());
        binary_file_type.insert("vector".to_string(), "vectortype.bin".to_string());

        Self {
            folder: "link".to_string(),
            binary_file_type,
            magic: b"SKYPYLINKER".to_vec(),
        }
    }
}

impl DatabaseLinker {
    pub fn discover_database_links(&self, root: Option<&Path>) -> Vec<DiscoveredDbLink> {
        let root = root
            .map(Path::to_path_buf)
            .unwrap_or_else(|| std::env::current_dir().unwrap_or_else(|_| PathBuf::from(".")));
        let mut discovered = Vec::new();

        for entry in WalkDir::new(root).into_iter().flatten() {
            if !entry.file_type().is_file() {
                continue;
            }
            let path = entry.path();
            if path.extension().and_then(|ext| ext.to_str()) != Some("bin") {
                continue;
            }
            if path
                .parent()
                .and_then(|parent| parent.file_name())
                .and_then(|name| name.to_str())
                != Some(self.folder.as_str())
            {
                continue;
            }
            let entries = self.read_link_metadata(path);
            discovered.extend(entries);
        }

        discovered
    }

    pub fn ensure_db_link_metadata(&self, path: &str, db_type: DatabaseType) -> Result<DbLink> {
        let resolved_db_path = self.resolve_db_path(path)?;
        let metadata_path = self.metadata_file_for_db(&resolved_db_path, db_type)?;
        if let Some(parent) = metadata_path.parent() {
            std::fs::create_dir_all(parent)?;
        }
        let mut existing_paths = Vec::new();
        if metadata_path.exists() {
            if let Ok(raw) = std::fs::read(&metadata_path) {
                if let Some((existing_type, paths)) = self.decode_binary_payload(&raw) {
                    if existing_type == db_type {
                        existing_paths = paths;
                    }
                }
            }
        }
        let normalized = resolved_db_path.to_string_lossy().to_string();
        if !existing_paths.contains(&normalized) {
            existing_paths.push(normalized.clone());
        }
        let payload = self.encode_binary_payload(db_type, &existing_paths)?;
        std::fs::write(&metadata_path, payload)?;

        Ok(DbLink {
            db_type: db_type.as_str().to_string(),
            path: normalized,
        })
    }

    fn resolve_db_path(&self, path: &str) -> Result<PathBuf> {
        let db_path = PathBuf::from(path);
        if db_path.is_absolute() {
            return Ok(db_path);
        }
        let cwd = std::env::current_dir()
            .map_err(|error| SkypydbError::database(format!("Failed to resolve cwd: {error}")))?;

        Ok((cwd.join(db_path))
            .canonicalize()
            .unwrap_or_else(|_| cwd.join(path)))
    }

    fn metadata_file_for_db(&self, db_path: &Path, db_type: DatabaseType) -> Result<PathBuf> {
        let link_dir = db_path
            .parent()
            .map(|parent| parent.join(&self.folder))
            .ok_or_else(|| SkypydbError::database("Database path has no parent directory"))?;
        let filename = self
            .binary_file_type
            .get(db_type.as_str())
            .ok_or_else(|| SkypydbError::database("Unknown database type"))?;

        Ok(link_dir.join(filename))
    }

    fn encode_binary_payload(&self, db_type: DatabaseType, db_paths: &[String]) -> Result<Vec<u8>> {
        let mut payload = Vec::new();
        payload.extend_from_slice(&self.magic);
        payload.push(db_type.type_code());
        payload.extend_from_slice(&(db_paths.len() as u32).to_be_bytes());

        for db_path in db_paths {
            let bytes = db_path.as_bytes();
            payload.extend_from_slice(&(bytes.len() as u32).to_be_bytes());
            payload.extend_from_slice(bytes);
        }

        Ok(payload)
    }

    fn decode_binary_payload(&self, raw: &[u8]) -> Option<(DatabaseType, Vec<String>)> {
        if raw.len() < self.magic.len() + 5 || !raw.starts_with(&self.magic) {
            return None;
        }
        let mut cursor = self.magic.len();
        let db_type = DatabaseType::from_type_code(*raw.get(cursor)?);
        let db_type = db_type?;
        cursor += 1;
        let count_bytes: [u8; 4] = raw.get(cursor..cursor + 4)?.try_into().ok()?;
        let count = u32::from_be_bytes(count_bytes) as usize;
        cursor += 4;
        let mut paths = Vec::new();

        for _ in 0..count {
            let len_bytes: [u8; 4] = raw.get(cursor..cursor + 4)?.try_into().ok()?;
            let path_len = u32::from_be_bytes(len_bytes) as usize;
            cursor += 4;
            let path_bytes = raw.get(cursor..cursor + path_len)?;
            let path = String::from_utf8(path_bytes.to_vec()).ok()?;
            cursor += path_len;
            if !path.is_empty() {
                paths.push(path);
            }
        }
        if cursor != raw.len() {
            return None;
        }

        Some((db_type, paths))
    }

    fn read_link_metadata(&self, path: &Path) -> Vec<DiscoveredDbLink> {
        let Ok(raw) = std::fs::read(path) else {
            return Vec::new();
        };
        let Some((db_type, db_paths)) = self.decode_binary_payload(&raw) else {
            return Vec::new();
        };

        db_paths
            .into_iter()
            .map(|db_path| DiscoveredDbLink {
                db_type: db_type.as_str().to_string(),
                path: db_path,
                metadata_file: path.to_string_lossy().to_string(),
            })
            .collect()
    }
}
