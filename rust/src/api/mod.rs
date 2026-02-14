//! High-level reactive and vector client APIs.

use crate::database::database_linker::{DatabaseLinker, DatabaseType};
use crate::database::reactive_database::ReactiveDatabase;
use crate::database::vector_database::{
    CollectionInfo, VectorDatabase, VectorGetResult, VectorQueryResult,
};
use crate::embeddings::{get_embedding_function, EmbeddingFunction};
use crate::errors::{Result, SkypydbError};
use crate::schema::Schema;
use crate::table::Table;
use chrono::Utc;
use serde_json::{Map, Value};
use std::collections::{BTreeMap, HashMap};
use std::path::Path;
use std::sync::{Arc, Mutex};

#[derive(Clone)]
pub struct ReactiveClient {
    path: String,
    db: ReactiveDatabase,
    _database_linker: DatabaseLinker,
}

impl ReactiveClient {
    pub fn new(
        path: impl Into<String>,
        encryption_key: Option<String>,
        salt: Option<Vec<u8>>,
        encrypted_fields: Option<Vec<String>>,
    ) -> Result<Self> {
        let path = path.into();
        if let Some(parent) = Path::new(&path).parent() {
            if !parent.as_os_str().is_empty() {
                std::fs::create_dir_all(parent)?;
            }
        }
        let db = ReactiveDatabase::new(path.clone(), encryption_key, salt, encrypted_fields)?;
        let database_linker = DatabaseLinker::default();
        database_linker.ensure_db_link_metadata(&path, DatabaseType::Reactive)?;

        Ok(Self {
            path,
            db,
            _database_linker: database_linker,
        })
    }

    pub fn path(&self) -> &str {
        &self.path
    }

    pub fn db(&self) -> &ReactiveDatabase {
        &self.db
    }

    pub fn create_tables(&self, schema: &Schema) -> Result<HashMap<String, Table>> {
        let table_names = schema.get_all_table_names();
        let existing_tables = table_names
            .iter()
            .filter(|table_name| self.db.table_exists(table_name))
            .cloned()
            .collect::<Vec<_>>();
        if !existing_tables.is_empty() {
            return Err(SkypydbError::table_already_exists(format!(
                "Tables already exist in the database: {}",
                existing_tables.join(", ")
            )));
        }
        let mut created: HashMap<String, Table> = HashMap::new();

        for table_name in table_names {
            let table_definition = schema.get_table_definition(&table_name).ok_or_else(|| {
                SkypydbError::validation(format!("Missing table definition for '{table_name}'"))
            })?;
            self.db.create_table(&table_name, table_definition)?;
            created.insert(table_name.clone(), Table::new(self.db.clone(), table_name)?);
        }

        Ok(created)
    }

    pub fn get_or_create_tables(&self, schema: &Schema) -> Result<HashMap<String, Table>> {
        let table_names = self.db.get_or_create_tables(schema)?;
        let mut tables: HashMap<String, Table> = HashMap::new();

        for table_name in table_names {
            tables.insert(table_name.clone(), Table::new(self.db.clone(), table_name)?);
        }

        Ok(tables)
    }

    pub fn get_table(&self, table_name: &str) -> Result<Table> {
        if !self.db.table_exists(table_name) {
            return Err(SkypydbError::table_not_found(format!(
                "Table '{table_name}' not found"
            )));
        }
        Table::new(self.db.clone(), table_name.to_string())
    }

    pub fn delete_table(&self, table_name: &str) -> Result<()> {
        if !self.db.table_exists(table_name) {
            return Err(SkypydbError::table_not_found(format!(
                "Table '{table_name}' not found"
            )));
        }

        self.db.delete_table(table_name)
    }

    pub fn close(&self) {
        self.db.close();
    }
}

#[derive(Clone)]
pub struct VectorClient {
    path: String,
    db: VectorDatabase,
    collections: Arc<Mutex<BTreeMap<String, Collection>>>,
    _database_linker: DatabaseLinker,
}

impl VectorClient {
    pub fn new(
        path: impl Into<String>,
        embedding_provider: &str,
        embedding_model_config: Option<Map<String, Value>>,
    ) -> Result<Self> {
        let path = path.into();
        if let Some(parent) = Path::new(&path).parent() {
            if !parent.as_os_str().is_empty() {
                std::fs::create_dir_all(parent)?;
            }
        }
        let embedding_function = get_embedding_function(
            &embedding_provider.to_lowercase().trim().replace('_', "-"),
            embedding_model_config.unwrap_or_default(),
        )?;
        let db = VectorDatabase::new(path.clone(), Some(embedding_function))?;
        let database_linker = DatabaseLinker::default();
        database_linker.ensure_db_link_metadata(&path, DatabaseType::Vector)?;

        Ok(Self {
            path,
            db,
            collections: Arc::new(Mutex::new(BTreeMap::new())),
            _database_linker: database_linker,
        })
    }

    pub fn with_embedding_function(
        path: impl Into<String>,
        embedding_function: Arc<dyn EmbeddingFunction>,
    ) -> Result<Self> {
        let path = path.into();
        if let Some(parent) = Path::new(&path).parent() {
            if !parent.as_os_str().is_empty() {
                std::fs::create_dir_all(parent)?;
            }
        }
        let db = VectorDatabase::new(path.clone(), Some(embedding_function))?;
        let database_linker = DatabaseLinker::default();
        database_linker.ensure_db_link_metadata(&path, DatabaseType::Vector)?;

        Ok(Self {
            path,
            db,
            collections: Arc::new(Mutex::new(BTreeMap::new())),
            _database_linker: database_linker,
        })
    }

    pub fn path(&self) -> &str {
        &self.path
    }

    pub fn db(&self) -> &VectorDatabase {
        &self.db
    }

    pub fn create_collection(
        &self,
        name: &str,
        metadata: Option<Value>,
        get_or_create: bool,
    ) -> Result<Collection> {
        if get_or_create {
            let _ = self.db.get_or_create_collection(name, metadata.clone())?;
        } else {
            self.db.create_collection(name, metadata.clone())?;
        }

        self.collection_from_db(name)
    }

    pub fn get_collection(&self, name: &str) -> Result<Collection> {
        if let Some(cached) = self
            .collections
            .lock()
            .map_err(|error| {
                SkypydbError::database(format!("Collection cache lock poisoned: {error}"))
            })?
            .get(name)
            .cloned()
        {
            return Ok(cached);
        }

        self.collection_from_db(name)
    }

    pub fn get_or_create_collection(
        &self,
        name: &str,
        metadata: Option<Value>,
    ) -> Result<Collection> {
        let _ = self.db.get_or_create_collection(name, metadata)?;

        self.collection_from_db(name)
    }

    pub fn list_collections(&self) -> Result<Vec<Collection>> {
        let infos = self.db.list_collections()?;
        let mut collections = Vec::new();

        for info in infos {
            collections.push(self.collection_from_info(info)?);
        }

        Ok(collections)
    }

    pub fn delete_collection(&self, name: &str) -> Result<()> {
        self.db.delete_collection(name)?;
        if let Ok(mut cache) = self.collections.lock() {
            cache.remove(name);
        }

        Ok(())
    }

    pub fn reset(&self) -> Result<bool> {
        self.db.reset()?;
        if let Ok(mut cache) = self.collections.lock() {
            cache.clear();
        }

        Ok(true)
    }

    pub fn heartbeat(&self) -> i64 {
        Utc::now().timestamp_nanos_opt().unwrap_or_default()
    }

    pub fn close(&self) {
        self.db.close();
        if let Ok(mut cache) = self.collections.lock() {
            cache.clear();
        }
    }

    fn collection_from_db(&self, name: &str) -> Result<Collection> {
        let info = self.db.get_collection(name)?.ok_or_else(|| {
            SkypydbError::collection_not_found(format!("Collection '{name}' not found"))
        })?;
        self.collection_from_info(info)
    }

    fn collection_from_info(&self, info: CollectionInfo) -> Result<Collection> {
        let mut cache = self.collections.lock().map_err(|error| {
            SkypydbError::database(format!("Collection cache lock poisoned: {error}"))
        })?;
        if let Some(existing) = cache.get(&info.name).cloned() {
            return Ok(existing);
        }
        let collection = Collection {
            db: self.db.clone(),
            name: info.name.clone(),
            metadata: info.metadata,
        };
        cache.insert(info.name, collection.clone());

        Ok(collection)
    }
}

#[derive(Clone)]
pub struct Collection {
    db: VectorDatabase,
    name: String,
    metadata: Value,
}

impl Collection {
    pub fn name(&self) -> &str {
        &self.name
    }

    pub fn metadata(&self) -> &Value {
        &self.metadata
    }

    pub fn add(
        &self,
        ids: Vec<String>,
        embeddings: Option<Vec<Vec<f32>>>,
        documents: Option<Vec<String>>,
        metadatas: Option<Vec<Value>>,
    ) -> Result<()> {
        self.db
            .add(&self.name, ids, embeddings, documents, metadatas)
            .map(|_| ())
    }

    #[allow(clippy::too_many_arguments)]
    pub fn query(
        &self,
        query_embeddings: Option<Vec<Vec<f32>>>,
        query_texts: Option<Vec<String>>,
        n_results: usize,
        where_filter: Option<Value>,
        where_document: Option<BTreeMap<String, String>>,
        include: Option<Vec<String>>,
    ) -> Result<VectorQueryResult> {
        self.db.query(
            &self.name,
            query_embeddings,
            query_texts,
            n_results,
            where_filter,
            where_document,
            include,
        )
    }

    #[allow(clippy::too_many_arguments)]
    pub fn get(
        &self,
        ids: Option<Vec<String>>,
        where_filter: Option<Value>,
        where_document: Option<BTreeMap<String, String>>,
        include: Option<Vec<String>>,
        limit: Option<usize>,
        offset: Option<usize>,
    ) -> Result<VectorGetResult> {
        let mut result = self
            .db
            .get(&self.name, ids, where_filter, where_document, include)?;

        if offset.is_some() || limit.is_some() {
            result.apply_window(offset.unwrap_or(0), limit);
        }

        Ok(result)
    }

    pub fn update(
        &self,
        ids: Vec<String>,
        embeddings: Option<Vec<Vec<f32>>>,
        documents: Option<Vec<String>>,
        metadatas: Option<Vec<Value>>,
    ) -> Result<()> {
        self.db
            .update(&self.name, ids, embeddings, documents, metadatas)
    }

    pub fn delete(
        &self,
        ids: Option<Vec<String>>,
        where_filter: Option<Value>,
        where_document: Option<BTreeMap<String, String>>,
    ) -> Result<()> {
        if ids.is_none() && where_filter.is_none() && where_document.is_none() {
            return Err(SkypydbError::validation(
                "delete() requires at least one of 'ids', 'where_filter', or 'where_document' to be provided.",
            ));
        }
        let _ = self
            .db
            .delete(&self.name, ids, where_filter, where_document)?;

        Ok(())
    }

    pub fn count(&self) -> Result<usize> {
        self.db.count(&self.name)
    }

    pub fn peek(&self, limit: usize) -> Result<VectorGetResult> {
        self.get(None, None, None, None, Some(limit), Some(0))
    }
}
