//! Vector SQLite database implementation.

use crate::embeddings::EmbeddingFunction;
use crate::errors::{Result, SkypydbError};
use crate::security::InputValidator;
use chrono::Utc;
use rusqlite::types::Value as SqlValue;
use rusqlite::{params, params_from_iter, Connection};
use serde::{Deserialize, Serialize};
use serde_json::{Map, Value};
use std::cmp::Ordering;
use std::collections::BTreeMap;
use std::path::Path;
use std::sync::{Arc, Mutex, MutexGuard};

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CollectionInfo {
    pub name: String,
    pub metadata: Value,
    pub created_at: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct VectorGetResult {
    pub ids: Vec<String>,
    pub embeddings: Option<Vec<Vec<f32>>>,
    pub documents: Option<Vec<Option<String>>>,
    pub metadatas: Option<Vec<Option<Value>>>,
}

impl VectorGetResult {
    pub fn apply_window(&mut self, offset: usize, limit: Option<usize>) {
        let end = limit.map(|value| offset + value);

        self.ids = slice_vector(&self.ids, offset, end);
        if let Some(embeddings) = self.embeddings.take() {
            self.embeddings = Some(slice_vector(&embeddings, offset, end));
        }
        if let Some(documents) = self.documents.take() {
            self.documents = Some(slice_vector(&documents, offset, end));
        }
        if let Some(metadatas) = self.metadatas.take() {
            self.metadatas = Some(slice_vector(&metadatas, offset, end));
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct VectorQueryResult {
    pub ids: Vec<Vec<String>>,
    pub embeddings: Option<Vec<Vec<Vec<f32>>>>,
    pub documents: Option<Vec<Vec<Option<String>>>>,
    pub metadatas: Option<Vec<Vec<Option<Value>>>>,
    pub distances: Option<Vec<Vec<f32>>>,
}

#[derive(Clone)]
pub struct VectorDatabase {
    path: String,
    conn: Arc<Mutex<Connection>>,
    embedding_function: Arc<Mutex<Option<Arc<dyn EmbeddingFunction>>>>,
}

impl VectorDatabase {
    pub fn new(
        path: impl Into<String>,
        embedding_function: Option<Arc<dyn EmbeddingFunction>>,
    ) -> Result<Self> {
        let path = path.into();
        if let Some(parent) = Path::new(&path).parent() {
            if !parent.as_os_str().is_empty() {
                std::fs::create_dir_all(parent)?;
            }
        }
        let connection = Connection::open(&path)?;
        connection.execute_batch("PRAGMA foreign_keys = ON;")?;
        let database = Self {
            path,
            conn: Arc::new(Mutex::new(connection)),
            embedding_function: Arc::new(Mutex::new(embedding_function)),
        };

        database.ensure_collections_table()?;

        Ok(database)
    }

    pub fn path(&self) -> &str {
        &self.path
    }

    fn lock_connection(&self) -> Result<MutexGuard<'_, Connection>> {
        self.conn
            .lock()
            .map_err(|error| SkypydbError::database(format!("Database lock poisoned: {error}")))
    }

    fn lock_embedding_function(
        &self,
    ) -> Result<MutexGuard<'_, Option<Arc<dyn EmbeddingFunction>>>> {
        self.embedding_function
            .lock()
            .map_err(|error| SkypydbError::embedding(format!("Embedding lock poisoned: {error}")))
    }

    fn current_embedding_function(&self) -> Result<Option<Arc<dyn EmbeddingFunction>>> {
        Ok(self.lock_embedding_function()?.clone())
    }

    pub fn set_embedding_function(
        &self,
        embedding_function: Arc<dyn EmbeddingFunction>,
    ) -> Result<()> {
        let mut guard = self.lock_embedding_function()?;
        *guard = Some(embedding_function);

        Ok(())
    }

    pub fn close(&self) {
        // Connection closes automatically on drop.
    }

    fn ensure_collections_table(&self) -> Result<()> {
        let conn = self.lock_connection()?;
        conn.execute(
            "
            CREATE TABLE IF NOT EXISTS _vector_collections (
                name TEXT PRIMARY KEY,
                metadata TEXT,
                created_at TEXT NOT NULL
            )
            ",
            [],
        )?;

        Ok(())
    }

    pub fn collection_exists(&self, name: &str) -> Result<bool> {
        let name = InputValidator::validate_table_name(name)?;
        let conn = self.lock_connection()?;
        let mut statement =
            conn.prepare("SELECT name FROM sqlite_master WHERE type='table' AND name=?")?;

        match statement.query_row([format!("vec_{name}")], |row| row.get::<_, String>(0)) {
            Ok(_) => Ok(true),
            Err(rusqlite::Error::QueryReturnedNoRows) => Ok(false),
            Err(error) => Err(SkypydbError::from(error)),
        }
    }

    pub fn create_collection(&self, name: &str, metadata: Option<Value>) -> Result<()> {
        let name = InputValidator::validate_table_name(name)?;
        if self.collection_exists(&name)? {
            return Err(SkypydbError::collection_already_exists(format!(
                "Collection '{name}' already exists"
            )));
        }
        let table_name = format!("vec_{name}");
        let metadata = metadata.unwrap_or_else(|| Value::Object(Map::new()));
        let conn = self.lock_connection()?;
        conn.execute(
            &format!(
                "
                CREATE TABLE [{table_name}] (
                    id TEXT PRIMARY KEY,
                    document TEXT,
                    embedding TEXT NOT NULL,
                    metadata TEXT,
                    created_at TEXT NOT NULL
                )
                "
            ),
            [],
        )?;
        conn.execute(
            "INSERT INTO _vector_collections (name, metadata, created_at) VALUES (?, ?, ?)",
            params![
                name,
                serde_json::to_string(&metadata)?,
                Utc::now().to_rfc3339()
            ],
        )?;

        Ok(())
    }

    pub fn get_collection(&self, name: &str) -> Result<Option<CollectionInfo>> {
        let name = InputValidator::validate_table_name(name)?;
        if !self.collection_exists(&name)? {
            return Ok(None);
        }
        let conn = self.lock_connection()?;
        let mut statement = conn.prepare("SELECT * FROM _vector_collections WHERE name = ?")?;

        match statement.query_row([name.clone()], |row| {
            let metadata_text: Option<String> = row.get("metadata")?;
            let metadata = metadata_text
                .as_deref()
                .map(|text| {
                    serde_json::from_str(text).unwrap_or_else(|_| Value::Object(Map::new()))
                })
                .unwrap_or_else(|| Value::Object(Map::new()));

            Ok(CollectionInfo {
                name: row.get("name")?,
                metadata,
                created_at: row.get("created_at")?,
            })
        }) {
            Ok(collection) => Ok(Some(collection)),
            Err(rusqlite::Error::QueryReturnedNoRows) => Ok(None),
            Err(error) => Err(SkypydbError::from(error)),
        }
    }

    pub fn get_or_create_collection(
        &self,
        name: &str,
        metadata: Option<Value>,
    ) -> Result<CollectionInfo> {
        let name = InputValidator::validate_table_name(name)?;
        if !self.collection_exists(&name)? {
            self.create_collection(&name, metadata)?;
        }

        self.get_collection(&name)?.ok_or_else(|| {
            SkypydbError::collection_not_found(format!("Collection '{name}' not found"))
        })
    }

    pub fn list_collections(&self) -> Result<Vec<CollectionInfo>> {
        let conn = self.lock_connection()?;
        let mut statement = conn.prepare("SELECT * FROM _vector_collections")?;
        let mut rows = statement.query([])?;
        let mut collections = Vec::new();

        while let Some(row) = rows.next()? {
            let metadata_text: Option<String> = row.get("metadata")?;
            let metadata = metadata_text
                .as_deref()
                .map(|text| {
                    serde_json::from_str(text).unwrap_or_else(|_| Value::Object(Map::new()))
                })
                .unwrap_or_else(|| Value::Object(Map::new()));

            collections.push(CollectionInfo {
                name: row.get("name")?,
                metadata,
                created_at: row.get("created_at")?,
            });
        }

        Ok(collections)
    }

    pub fn delete_collection(&self, name: &str) -> Result<()> {
        let name = InputValidator::validate_table_name(name)?;
        if !self.collection_exists(&name)? {
            return Err(SkypydbError::collection_not_found(format!(
                "Collection '{name}' not found"
            )));
        }
        let table_name = format!("vec_{name}");
        let conn = self.lock_connection()?;
        conn.execute(&format!("DROP TABLE [{table_name}]"), [])?;
        conn.execute("DELETE FROM _vector_collections WHERE name = ?", [name])?;

        Ok(())
    }

    pub fn count(&self, collection_name: &str) -> Result<usize> {
        let collection_name = InputValidator::validate_table_name(collection_name)?;
        if !self.collection_exists(&collection_name)? {
            return Err(SkypydbError::collection_not_found(format!(
                "Collection '{collection_name}' not found"
            )));
        }
        let conn = self.lock_connection()?;
        let mut statement =
            conn.prepare(&format!("SELECT COUNT(*) FROM [vec_{collection_name}]"))?;
        let count: i64 = statement.query_row([], |row| row.get(0))?;

        Ok(count.max(0) as usize)
    }

    pub fn add(
        &self,
        collection_name: &str,
        ids: Vec<String>,
        embeddings: Option<Vec<Vec<f32>>>,
        documents: Option<Vec<String>>,
        metadatas: Option<Vec<Value>>,
    ) -> Result<Vec<String>> {
        let collection_name = InputValidator::validate_table_name(collection_name)?;
        if !self.collection_exists(&collection_name)? {
            return Err(SkypydbError::collection_not_found(format!(
                "Collection '{collection_name}' not found"
            )));
        }
        if embeddings.is_none() && documents.is_none() {
            return Err(SkypydbError::validation(
                "Either embeddings or documents must be provided",
            ));
        }
        let embeddings = if let Some(embeddings) = embeddings {
            embeddings
        } else {
            let Some(embedding_function) = self.current_embedding_function()? else {
                return Err(SkypydbError::embedding(
                    "Documents provided but no embedding function set. Either provide embeddings directly or set an embedding function.",
                ));
            };
            let Some(documents_ref) = documents.as_ref() else {
                return Err(SkypydbError::validation(
                    "Either embeddings or documents must be provided",
                ));
            };
            embedding_function.embed(documents_ref)?
        };
        let n_items = ids.len();
        if embeddings.len() != n_items {
            return Err(SkypydbError::validation(format!(
                "Number of embeddings ({}) doesn't match number of IDs ({n_items})",
                embeddings.len()
            )));
        }
        if let Some(documents) = &documents {
            if documents.len() != n_items {
                return Err(SkypydbError::validation(format!(
                    "Number of documents ({}) doesn't match number of IDs ({n_items})",
                    documents.len()
                )));
            }
        }
        if let Some(metadatas) = &metadatas {
            if metadatas.len() != n_items {
                return Err(SkypydbError::validation(format!(
                    "Number of metadatas ({}) doesn't match number of IDs ({n_items})",
                    metadatas.len()
                )));
            }
        }
        let conn = self.lock_connection()?;

        for (index, item_id) in ids.iter().enumerate() {
            let document = documents
                .as_ref()
                .and_then(|values| values.get(index))
                .cloned();
            let metadata = metadatas
                .as_ref()
                .and_then(|values| values.get(index))
                .cloned();

            conn.execute(
                &format!(
                    "
                    INSERT OR REPLACE INTO [vec_{collection_name}] (id, document, embedding, metadata, created_at)
                    VALUES (?, ?, ?, ?, ?)
                    "
                ),
                params![
                    item_id,
                    document,
                    serde_json::to_string(&embeddings[index])?,
                    metadata.map(|value| serde_json::to_string(&value)).transpose()?,
                    Utc::now().to_rfc3339(),
                ],
            )?;
        }

        Ok(ids)
    }

    pub fn update(
        &self,
        collection_name: &str,
        ids: Vec<String>,
        embeddings: Option<Vec<Vec<f32>>>,
        documents: Option<Vec<String>>,
        metadatas: Option<Vec<Value>>,
    ) -> Result<()> {
        let collection_name = InputValidator::validate_table_name(collection_name)?;
        if !self.collection_exists(&collection_name)? {
            return Err(SkypydbError::collection_not_found(format!(
                "Collection '{collection_name}' not found"
            )));
        }
        if let Some(embeddings) = &embeddings {
            if embeddings.len() != ids.len() {
                return Err(SkypydbError::validation(format!(
                    "Number of embeddings ({}) doesn't match number of IDs ({})",
                    embeddings.len(),
                    ids.len()
                )));
            }
        }
        if let Some(documents) = &documents {
            if documents.len() != ids.len() {
                return Err(SkypydbError::validation(format!(
                    "Number of documents ({}) doesn't match number of IDs ({})",
                    documents.len(),
                    ids.len()
                )));
            }
        }
        if let Some(metadatas) = &metadatas {
            if metadatas.len() != ids.len() {
                return Err(SkypydbError::validation(format!(
                    "Number of metadatas ({}) doesn't match number of IDs ({})",
                    metadatas.len(),
                    ids.len()
                )));
            }
        }
        let embeddings = if embeddings.is_none() && documents.is_some() {
            let Some(embedding_function) = self.current_embedding_function()? else {
                return Err(SkypydbError::embedding(
                    "Documents provided but no embedding function set.",
                ));
            };
            Some(embedding_function.embed(documents.as_ref().expect("documents present"))?)
        } else {
            embeddings
        };
        let conn = self.lock_connection()?;

        for (index, item_id) in ids.iter().enumerate() {
            let mut updates = Vec::new();
            let mut params = Vec::<SqlValue>::new();
            if let Some(embeddings) = &embeddings {
                updates.push("embedding = ?".to_string());
                params.push(SqlValue::Text(serde_json::to_string(&embeddings[index])?));
            }
            if let Some(documents) = &documents {
                updates.push("document = ?".to_string());
                params.push(SqlValue::Text(documents[index].clone()));
            }
            if let Some(metadatas) = &metadatas {
                updates.push("metadata = ?".to_string());
                params.push(SqlValue::Text(serde_json::to_string(&metadatas[index])?));
            }
            if updates.is_empty() {
                continue;
            }
            params.push(SqlValue::Text(item_id.clone()));
            let query = format!(
                "UPDATE [vec_{collection_name}] SET {} WHERE id = ?",
                updates.join(", ")
            );
            conn.execute(&query, params_from_iter(params.iter()))?;
        }

        Ok(())
    }

    pub fn get(
        &self,
        collection_name: &str,
        ids: Option<Vec<String>>,
        where_filter: Option<Value>,
        where_document: Option<BTreeMap<String, String>>,
        include: Option<Vec<String>>,
    ) -> Result<VectorGetResult> {
        let collection_name = InputValidator::validate_table_name(collection_name)?;
        if !self.collection_exists(&collection_name)? {
            return Err(SkypydbError::collection_not_found(format!(
                "Collection '{collection_name}' not found"
            )));
        }
        let include = include.unwrap_or_else(|| {
            vec![
                "embeddings".to_string(),
                "documents".to_string(),
                "metadatas".to_string(),
            ]
        });
        let include_embeddings = include.iter().any(|field| field == "embeddings");
        let include_documents = include.iter().any(|field| field == "documents");
        let include_metadatas = include.iter().any(|field| field == "metadatas");
        let items = self.get_items_by_ids_or_all(&collection_name, ids)?;
        let mut result = VectorGetResult {
            ids: Vec::new(),
            embeddings: include_embeddings.then_some(Vec::new()),
            documents: include_documents.then_some(Vec::new()),
            metadatas: include_metadatas.then_some(Vec::new()),
        };

        for item in items {
            if !matches_filters(&item, where_filter.as_ref(), where_document.as_ref()) {
                continue;
            }
            result.ids.push(item.id.clone());
            if let Some(embeddings) = result.embeddings.as_mut() {
                embeddings.push(item.embedding.clone());
            }
            if let Some(documents) = result.documents.as_mut() {
                documents.push(item.document.clone());
            }
            if let Some(metadatas) = result.metadatas.as_mut() {
                metadatas.push(item.metadata.clone());
            }
        }

        Ok(result)
    }

    pub fn query(
        &self,
        collection_name: &str,
        query_embeddings: Option<Vec<Vec<f32>>>,
        query_texts: Option<Vec<String>>,
        n_results: usize,
        where_filter: Option<Value>,
        where_document: Option<BTreeMap<String, String>>,
        include: Option<Vec<String>>,
    ) -> Result<VectorQueryResult> {
        let collection_name = InputValidator::validate_table_name(collection_name)?;
        if !self.collection_exists(&collection_name)? {
            return Err(SkypydbError::collection_not_found(format!(
                "Collection '{collection_name}' not found"
            )));
        }
        if query_embeddings.is_none() && query_texts.is_none() {
            return Err(SkypydbError::validation(
                "Either query_embeddings or query_texts must be provided",
            ));
        }
        let query_embeddings = if let Some(query_embeddings) = query_embeddings {
            query_embeddings
        } else {
            let Some(embedding_function) = self.current_embedding_function()? else {
                return Err(SkypydbError::embedding(
                    "Query texts provided but no embedding function set.",
                ));
            };
            embedding_function.embed(query_texts.as_ref().expect("query_texts present"))?
        };
        let include = include.unwrap_or_else(|| {
            vec![
                "embeddings".to_string(),
                "documents".to_string(),
                "metadatas".to_string(),
                "distances".to_string(),
            ]
        });
        let include_embeddings = include.iter().any(|field| field == "embeddings");
        let include_documents = include.iter().any(|field| field == "documents");
        let include_metadatas = include.iter().any(|field| field == "metadatas");
        let include_distances = include.iter().any(|field| field == "distances");
        let all_items = self.get_all_items(&collection_name)?;
        let mut result = VectorQueryResult {
            ids: Vec::new(),
            embeddings: include_embeddings.then_some(Vec::new()),
            documents: include_documents.then_some(Vec::new()),
            metadatas: include_metadatas.then_some(Vec::new()),
            distances: include_distances.then_some(Vec::new()),
        };

        for query_embedding in query_embeddings {
            let mut scored_items = Vec::<(VectorItem, f32)>::new();

            for item in &all_items {
                if !matches_filters(item, where_filter.as_ref(), where_document.as_ref()) {
                    continue;
                }

                let similarity = cosine_similarity(&query_embedding, &item.embedding)?;
                let distance = 1.0 - similarity;
                scored_items.push((item.clone(), distance));
            }

            scored_items
                .sort_by(|left, right| left.1.partial_cmp(&right.1).unwrap_or(Ordering::Equal));
            let top_items = scored_items.into_iter().take(n_results);
            let mut query_ids = Vec::new();
            let mut query_embeddings_result = Vec::new();
            let mut query_documents = Vec::new();
            let mut query_metadatas = Vec::new();
            let mut query_distances = Vec::new();

            for (item, distance) in top_items {
                query_ids.push(item.id);
                query_embeddings_result.push(item.embedding);
                query_documents.push(item.document);
                query_metadatas.push(item.metadata);
                query_distances.push(distance);
            }
            result.ids.push(query_ids);
            if let Some(embeddings) = result.embeddings.as_mut() {
                embeddings.push(query_embeddings_result);
            }
            if let Some(documents) = result.documents.as_mut() {
                documents.push(query_documents);
            }
            if let Some(metadatas) = result.metadatas.as_mut() {
                metadatas.push(query_metadatas);
            }
            if let Some(distances) = result.distances.as_mut() {
                distances.push(query_distances);
            }
        }

        Ok(result)
    }

    pub fn delete(
        &self,
        collection_name: &str,
        ids: Option<Vec<String>>,
        where_filter: Option<Value>,
        where_document: Option<BTreeMap<String, String>>,
    ) -> Result<usize> {
        let collection_name = InputValidator::validate_table_name(collection_name)?;
        if !self.collection_exists(&collection_name)? {
            return Err(SkypydbError::collection_not_found(format!(
                "Collection '{collection_name}' not found"
            )));
        }
        if let Some(ids) = ids {
            if ids.is_empty() {
                return Ok(0);
            }
            let placeholders = std::iter::repeat("?")
                .take(ids.len())
                .collect::<Vec<_>>()
                .join(", ");
            let params = ids.into_iter().map(SqlValue::Text).collect::<Vec<_>>();
            let conn = self.lock_connection()?;
            let affected = conn.execute(
                &format!("DELETE FROM [vec_{collection_name}] WHERE id IN ({placeholders})"),
                params_from_iter(params.iter()),
            )?;
            return Ok(affected);
        }
        let items = self.get_all_items(&collection_name)?;
        let ids_to_delete = items
            .iter()
            .filter(|item| matches_filters(item, where_filter.as_ref(), where_document.as_ref()))
            .map(|item| item.id.clone())
            .collect::<Vec<_>>();
        if ids_to_delete.is_empty() {
            return Ok(0);
        }
        let placeholders = std::iter::repeat("?")
            .take(ids_to_delete.len())
            .collect::<Vec<_>>()
            .join(", ");
        let params = ids_to_delete
            .into_iter()
            .map(SqlValue::Text)
            .collect::<Vec<_>>();

        let conn = self.lock_connection()?;
        let affected = conn.execute(
            &format!("DELETE FROM [vec_{collection_name}] WHERE id IN ({placeholders})"),
            params_from_iter(params.iter()),
        )?;

        Ok(affected)
    }

    pub fn reset(&self) -> Result<()> {
        let collections = self.list_collections()?;

        for collection in collections {
            self.delete_collection(&collection.name)?;
        }
        Ok(())
    }

    fn get_items_by_ids_or_all(
        &self,
        collection_name: &str,
        ids: Option<Vec<String>>,
    ) -> Result<Vec<VectorItem>> {
        let conn = self.lock_connection()?;
        if let Some(ids) = ids {
            if ids.is_empty() {
                return Ok(Vec::new());
            }
            let placeholders = std::iter::repeat("?")
                .take(ids.len())
                .collect::<Vec<_>>()
                .join(", ");
            let params = ids.into_iter().map(SqlValue::Text).collect::<Vec<_>>();
            let mut statement = conn.prepare(&format!(
                "SELECT * FROM [vec_{collection_name}] WHERE id IN ({placeholders})"
            ))?;
            let mut rows = statement.query(params_from_iter(params.iter()))?;
            let mut items = Vec::new();

            while let Some(row) = rows.next()? {
                items.push(parse_vector_item(row)?);
            }
            return Ok(items);
        }

        drop(conn);
        self.get_all_items(collection_name)
    }

    fn get_all_items(&self, collection_name: &str) -> Result<Vec<VectorItem>> {
        let conn = self.lock_connection()?;
        let mut statement = conn.prepare(&format!("SELECT * FROM [vec_{collection_name}]"))?;
        let mut rows = statement.query([])?;
        let mut items = Vec::new();

        while let Some(row) = rows.next()? {
            items.push(parse_vector_item(row)?);
        }

        Ok(items)
    }
}

#[derive(Debug, Clone)]
struct VectorItem {
    id: String,
    document: Option<String>,
    embedding: Vec<f32>,
    metadata: Option<Value>,
}

fn parse_vector_item(row: &rusqlite::Row<'_>) -> Result<VectorItem> {
    let embedding_text: String = row.get("embedding")?;
    let metadata_text: Option<String> = row.get("metadata")?;
    let metadata = metadata_text
        .as_deref()
        .map(|text| serde_json::from_str(text))
        .transpose()?;

    Ok(VectorItem {
        id: row.get("id")?,
        document: row.get("document")?,
        embedding: serde_json::from_str::<Vec<f32>>(&embedding_text)?,
        metadata,
    })
}

fn matches_filters(
    item: &VectorItem,
    where_filter: Option<&Value>,
    where_document: Option<&BTreeMap<String, String>>,
) -> bool {
    if let Some(filter) = where_filter {
        if !matches_metadata_filter(item.metadata.as_ref(), filter) {
            return false;
        }
    }
    if let Some(filter) = where_document {
        let document = item.document.as_deref().unwrap_or("");
        for (operator, value) in filter {
            match operator.as_str() {
                "$contains" => {
                    if !document.contains(value) {
                        return false;
                    }
                }
                "$not_contains" => {
                    if document.contains(value) {
                        return false;
                    }
                }
                _ => return false,
            }
        }
    }
    true
}

fn matches_metadata_filter(metadata: Option<&Value>, filter: &Value) -> bool {
    let Value::Object(filter_map) = filter else {
        return false;
    };
    let empty = Map::new();
    let metadata_map = metadata.and_then(Value::as_object).unwrap_or(&empty);

    for (key, value) in filter_map {
        if key == "$and" {
            let Some(conditions) = value.as_array() else {
                return false;
            };
            if !conditions
                .iter()
                .all(|condition| matches_metadata_filter(metadata, condition))
            {
                return false;
            }
            continue;
        }
        if key == "$or" {
            let Some(conditions) = value.as_array() else {
                return false;
            };
            if !conditions
                .iter()
                .any(|condition| matches_metadata_filter(metadata, condition))
            {
                return false;
            }
            continue;
        }
        let null_value = Value::Null;
        let metadata_value = metadata_map.get(key).unwrap_or(&null_value);
        if let Value::Object(operators) = value {
            let operators_are_comparisons =
                operators.keys().all(|operator| operator.starts_with('$'));
            if operators_are_comparisons {
                if !operators.iter().all(|(operator, comparison_value)| {
                    compare_with_operator(metadata_value, operator, comparison_value)
                }) {
                    return false;
                }
                continue;
            }
        }
        if metadata_value != value {
            return false;
        }
    }
    true
}

fn compare_with_operator(left: &Value, operator: &str, right: &Value) -> bool {
    match operator {
        "$eq" => left == right,
        "$ne" => left != right,
        "$gt" => compare_order(left, right, Ordering::Greater),
        "$gte" => {
            compare_order(left, right, Ordering::Greater)
                || compare_order(left, right, Ordering::Equal)
        }
        "$lt" => compare_order(left, right, Ordering::Less),
        "$lte" => {
            compare_order(left, right, Ordering::Less)
                || compare_order(left, right, Ordering::Equal)
        }
        "$in" => right
            .as_array()
            .map(|values| values.iter().any(|value| value == left))
            .unwrap_or(false),
        "$nin" => right
            .as_array()
            .map(|values| values.iter().all(|value| value != left))
            .unwrap_or(true),
        _ => false,
    }
}

fn compare_order(left: &Value, right: &Value, expected: Ordering) -> bool {
    let ordering = if let (Some(left), Some(right)) = (left.as_f64(), right.as_f64()) {
        left.partial_cmp(&right)
    } else if let (Some(left), Some(right)) = (left.as_str(), right.as_str()) {
        Some(left.cmp(right))
    } else if let (Some(left), Some(right)) = (left.as_bool(), right.as_bool()) {
        Some(left.cmp(&right))
    } else {
        None
    };

    ordering == Some(expected)
}

fn cosine_similarity(left: &[f32], right: &[f32]) -> Result<f32> {
    if left.len() != right.len() {
        return Err(SkypydbError::vector_search(format!(
            "Vector dimensions don't match: {} vs {}",
            left.len(),
            right.len()
        )));
    }
    let dot_product = left
        .iter()
        .zip(right.iter())
        .map(|(left, right)| left * right)
        .sum::<f32>();
    let left_norm = left.iter().map(|value| value * value).sum::<f32>().sqrt();
    let right_norm = right.iter().map(|value| value * value).sum::<f32>().sqrt();
    if left_norm == 0.0 || right_norm == 0.0 {
        return Ok(0.0);
    }

    Ok(dot_product / (left_norm * right_norm))
}

fn slice_vector<T: Clone>(values: &[T], offset: usize, end: Option<usize>) -> Vec<T> {
    if offset >= values.len() {
        return Vec::new();
    }
    let end = end.unwrap_or(values.len()).min(values.len());
    values[offset..end].to_vec()
}

#[cfg(test)]
mod tests {
    use super::VectorDatabase;
    use crate::embeddings::EmbeddingFunction;
    use crate::errors::Result;
    use std::sync::Arc;
    use tempfile::tempdir;

    struct DummyEmbedding;

    impl EmbeddingFunction for DummyEmbedding {
        fn embed(&self, texts: &[String]) -> Result<Vec<Vec<f32>>> {
            Ok(texts
                .iter()
                .map(|text| {
                    vec![
                        text.len() as f32,
                        text.bytes().map(|byte| byte as u64).sum::<u64>() as f32,
                    ]
                })
                .collect())
        }
    }

    #[test]
    fn vector_add_and_query_roundtrip() {
        let dir = tempdir().expect("tempdir");
        let db_path = dir.path().join("vector.db");
        let db = VectorDatabase::new(db_path.to_string_lossy(), Some(Arc::new(DummyEmbedding)))
            .expect("vector db");

        db.get_or_create_collection("docs", None)
            .expect("collection");

        db.add(
            "docs",
            vec!["doc1".to_string(), "doc2".to_string()],
            None,
            Some(vec!["hello".to_string(), "world".to_string()]),
            None,
        )
        .expect("add");
        let result = db
            .query(
                "docs",
                None,
                Some(vec!["hello".to_string()]),
                1,
                None,
                None,
                None,
            )
            .expect("query");
        assert_eq!(result.ids.len(), 1);
        assert_eq!(result.ids[0].len(), 1);
    }
}
