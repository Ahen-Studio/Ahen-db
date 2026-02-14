//! Reactive SQLite database implementation.

use crate::errors::{Result, SkypydbError};
use crate::schema::{Schema, TableDefinition, Validator};
use crate::security::{EncryptionManager, InputValidator};
use base64::Engine;
use chrono::Utc;
use rusqlite::types::{Value as SqlValue, ValueRef};
use rusqlite::{params, params_from_iter, Connection};
use serde_json::{Map, Number, Value};
use std::collections::HashSet;
use std::path::Path;
use std::sync::{Arc, Mutex, MutexGuard};
use uuid::Uuid;

pub type DataMap = Map<String, Value>;

#[derive(Clone)]
pub struct ReactiveDatabase {
    path: String,
    conn: Arc<Mutex<Connection>>,
    encryption_manager: EncryptionManager,
    encrypted_fields: Vec<String>,
}

impl ReactiveDatabase {
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
        if encryption_key.is_some() && encrypted_fields.is_none() {
            return Err(SkypydbError::validation(
                "encrypted_fields must be explicitly set when encryption_key is provided; use [] to disable encryption.",
            ));
        }
        let connection = Connection::open(&path)?;
        connection.execute_batch("PRAGMA foreign_keys = ON;")?;
        let encryption_manager =
            EncryptionManager::new(encryption_key.as_deref(), salt.as_deref())?;
        let database = Self {
            path,
            conn: Arc::new(Mutex::new(connection)),
            encryption_manager,
            encrypted_fields: encrypted_fields.unwrap_or_default(),
        };

        database.check_config_table()?;

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

    pub fn close(&self) {
        // Connection closes automatically on drop.
    }

    pub fn check_config_table(&self) -> Result<()> {
        let conn = self.lock_connection()?;
        conn.execute(
            "
            CREATE TABLE IF NOT EXISTS _skypy_config (
                table_name TEXT PRIMARY KEY,
                config TEXT NOT NULL,
                created_at TEXT NOT NULL
            )
            ",
            [],
        )?;

        Ok(())
    }

    pub fn table_exists(&self, table_name: &str) -> bool {
        let Ok(validated_table_name) = InputValidator::validate_table_name(table_name) else {
            return false;
        };
        let Ok(conn) = self.lock_connection() else {
            return false;
        };
        let mut statement =
            match conn.prepare("SELECT name FROM sqlite_master WHERE type='table' AND name=?") {
                Ok(statement) => statement,
                Err(_) => return false,
            };

        match statement.query_row([validated_table_name], |row| row.get::<_, String>(0)) {
            Ok(_) => true,
            Err(rusqlite::Error::QueryReturnedNoRows) => false,
            Err(_) => false,
        }
    }

    pub fn get_all_tables_names(&self) -> Result<Vec<String>> {
        let conn = self.lock_connection()?;
        let mut statement = conn.prepare(
            "SELECT name FROM sqlite_master WHERE type='table' AND name NOT LIKE 'sqlite_%' AND name != '_skypy_config'",
        )?;
        let mut rows = statement.query([])?;
        let mut table_names = Vec::new();

        while let Some(row) = rows.next()? {
            table_names.push(row.get::<_, String>(0)?);
        }

        Ok(table_names)
    }

    pub fn get_table_columns_names(&self, table_name: &str) -> Result<Vec<String>> {
        let table_name = InputValidator::validate_table_name(table_name)?;
        if !self.table_exists(&table_name) {
            return Err(SkypydbError::table_not_found(format!(
                "Table '{table_name}' not found"
            )));
        }
        let conn = self.lock_connection()?;
        let mut statement = conn.prepare(&format!("PRAGMA table_info([{table_name}])"))?;
        let mut rows = statement.query([])?;
        let mut columns = Vec::new();

        while let Some(row) = rows.next()? {
            columns.push(row.get::<_, String>(1)?);
        }

        Ok(columns)
    }

    pub fn get_all_data(&self, table_name: &str) -> Result<Vec<DataMap>> {
        let table_name = InputValidator::validate_table_name(table_name)?;
        if !self.table_exists(&table_name) {
            return Err(SkypydbError::table_not_found(format!(
                "Table '{table_name}' not found"
            )));
        }
        let conn = self.lock_connection()?;
        let query = format!("SELECT * FROM [{table_name}]");
        let mut statement = conn.prepare(&query)?;
        let column_names = statement
            .column_names()
            .iter()
            .map(|name| (*name).to_string())
            .collect::<Vec<_>>();
        let mut rows = statement.query([])?;
        let mut results = Vec::new();

        while let Some(row) = rows.next()? {
            let map = row_to_map(row, &column_names)?;
            results.push(self.decrypt_data(&map)?);
        }

        Ok(results)
    }

    pub fn create_table(&self, table_name: &str, table_definition: &TableDefinition) -> Result<()> {
        let table_name = InputValidator::validate_table_name(table_name)?;

        for column_name in table_definition.columns.keys() {
            InputValidator::validate_column_name(column_name)?;
        }
        if self.table_exists(&table_name) {
            return Err(SkypydbError::table_already_exists(format!(
                "Table '{table_name}' already exists"
            )));
        }
        let mut configured_definition = table_definition.clone();
        configured_definition.table_name = Some(table_name.clone());
        let sql_columns = configured_definition.get_sql_columns().join(", ");
        let create_query = format!("CREATE TABLE [{table_name}] ({sql_columns})");
        let conn = self.lock_connection()?;
        conn.execute(&create_query, [])?;

        for index_sql in configured_definition.get_sql_indexes() {
            conn.execute(&index_sql, [])?;
        }
        let config = self.table_def_to_config(&configured_definition)?;
        drop(conn);
        self.save_table_config(&table_name, &config)?;

        Ok(())
    }

    pub fn delete_table(&self, table_name: &str) -> Result<()> {
        let table_name = InputValidator::validate_table_name(table_name)?;
        if !self.table_exists(&table_name) {
            return Err(SkypydbError::table_not_found(format!(
                "Table '{table_name}' not found"
            )));
        }
        let conn = self.lock_connection()?;
        let query = format!("DROP TABLE [{table_name}]");
        conn.execute(&query, [])?;
        drop(conn);

        self.delete_table_config(&table_name)?;

        Ok(())
    }

    pub fn get_or_create_tables(&self, schema: &Schema) -> Result<Vec<String>> {
        let mut created_tables = Vec::new();

        for table_name in schema.get_all_table_names() {
            let Some(table_definition) = schema.get_table_definition(&table_name) else {
                continue;
            };
            if !self.table_exists(&table_name) {
                self.create_table(&table_name, table_definition)?;
            }
            created_tables.push(table_name);
        }

        Ok(created_tables)
    }

    pub fn get_table_config(&self, table_name: &str) -> Result<Option<Value>> {
        let table_name = InputValidator::validate_table_name(table_name)?;
        let conn = self.lock_connection()?;
        let mut statement =
            conn.prepare("SELECT config FROM _skypy_config WHERE table_name = ?")?;

        match statement.query_row([table_name], |row| row.get::<_, String>(0)) {
            Ok(config_text) => {
                let parsed = serde_json::from_str::<Value>(&config_text)?;

                Ok(Some(parsed))
            }
            Err(rusqlite::Error::QueryReturnedNoRows) => Ok(None),
            Err(error) => Err(SkypydbError::from(error)),
        }
    }

    pub fn save_table_config(&self, table_name: &str, config: &Value) -> Result<()> {
        let table_name = InputValidator::validate_table_name(table_name)?;
        let normalized = self.normalize_config(config);
        let config_text = serde_json::to_string(&normalized)?;
        let conn = self.lock_connection()?;
        conn.execute(
            "
            INSERT OR REPLACE INTO _skypy_config (table_name, config, created_at)
            VALUES (?, ?, ?)
            ",
            params![table_name, config_text, Utc::now().to_rfc3339()],
        )?;

        Ok(())
    }

    pub fn delete_table_config(&self, table_name: &str) -> Result<()> {
        let table_name = InputValidator::validate_table_name(table_name)?;
        let conn = self.lock_connection()?;
        conn.execute(
            "DELETE FROM _skypy_config WHERE table_name = ?",
            [table_name],
        )?;

        Ok(())
    }

    pub fn validate_data_with_config(&self, table_name: &str, data: &DataMap) -> Result<DataMap> {
        let Some(config) = self.get_table_config(table_name)? else {
            return Ok(data.clone());
        };
        let mut validated = Map::new();
        let Value::Object(config_map) = config else {
            return Ok(data.clone());
        };

        for (key, value) in data {
            let Some(expected) = config_map.get(key) else {
                validated.insert(key.clone(), value.clone());
                continue;
            };
            let (expected_type, optional) = match expected {
                Value::String(type_name) => (type_name.clone(), false),
                Value::Object(descriptor) => {
                    let type_name = descriptor
                        .get("type")
                        .and_then(Value::as_str)
                        .unwrap_or("str")
                        .to_string();
                    let optional = descriptor
                        .get("optional")
                        .and_then(Value::as_bool)
                        .unwrap_or(false);
                    (type_name, optional)
                }
                _ => ("str".to_string(), false),
            };
            if value.is_null() && optional {
                validated.insert(key.clone(), Value::Null);
                continue;
            }
            if expected_type == "auto" || expected_type == "id" {
                continue;
            }
            let converted = match expected_type.as_str() {
                "str" => Value::String(value_to_string(value)),
                "int" => {
                    if let Some(i) = value.as_i64() {
                        Value::Number(Number::from(i))
                    } else {
                        let parsed = value_to_string(value).parse::<i64>().map_err(|_| {
                            SkypydbError::validation(format!(
                                "Invalid type for column '{key}': expected int"
                            ))
                        })?;
                        Value::Number(Number::from(parsed))
                    }
                }
                "float" => {
                    if let Some(f) = value.as_f64() {
                        Value::Number(Number::from_f64(f).ok_or_else(|| {
                            SkypydbError::validation(format!(
                                "Invalid float value for column '{key}'"
                            ))
                        })?)
                    } else {
                        let parsed = value_to_string(value).parse::<f64>().map_err(|_| {
                            SkypydbError::validation(format!(
                                "Invalid type for column '{key}': expected float"
                            ))
                        })?;
                        Value::Number(Number::from_f64(parsed).ok_or_else(|| {
                            SkypydbError::validation(format!(
                                "Invalid float value for column '{key}'"
                            ))
                        })?)
                    }
                }
                "bool" => {
                    let bool_value = if let Some(boolean) = value.as_bool() {
                        boolean
                    } else {
                        matches!(
                            value_to_string(value).to_lowercase().as_str(),
                            "true" | "1" | "yes"
                        )
                    };
                    Value::Bool(bool_value)
                }
                _ => Value::String(value_to_string(value)),
            };

            validated.insert(key.clone(), converted);
        }

        Ok(validated)
    }

    pub fn add_data(&self, table_name: &str, data: &DataMap, generate_id: bool) -> Result<String> {
        let table_name = InputValidator::validate_table_name(table_name)?;
        let mut validated_data = InputValidator::validate_data_dict(data)?;
        if !self.table_exists(&table_name) {
            return Err(SkypydbError::table_not_found(format!(
                "Table '{table_name}' not found"
            )));
        }
        if generate_id {
            validated_data.insert("id".to_string(), Value::String(Uuid::new_v4().to_string()));
        }
        if !validated_data.contains_key("created_at") {
            validated_data.insert(
                "created_at".to_string(),
                Value::String(Utc::now().to_rfc3339()),
            );
        }
        let columns_to_add = validated_data
            .keys()
            .filter(|column| *column != "id" && *column != "created_at")
            .cloned()
            .collect::<Vec<_>>();

        self.add_columns_if_needed(&table_name, &columns_to_add)?;

        let encrypted_data = self.encrypt_data(&validated_data)?;
        let columns = encrypted_data.keys().cloned().collect::<Vec<_>>();
        let placeholders = std::iter::repeat("?")
            .take(columns.len())
            .collect::<Vec<_>>()
            .join(", ");
        let column_names = columns
            .iter()
            .map(|column| format!("[{column}]"))
            .collect::<Vec<_>>()
            .join(", ");
        let query = format!("INSERT INTO [{table_name}] ({column_names}) VALUES ({placeholders})");
        let params = columns
            .iter()
            .map(|column| {
                json_value_to_insert_sql(encrypted_data.get(column).unwrap_or(&Value::Null))
            })
            .collect::<Vec<_>>();
        let conn = self.lock_connection()?;
        conn.execute(&query, params_from_iter(params.iter()))?;
        let inserted_id = validated_data
            .get("id")
            .and_then(Value::as_str)
            .ok_or_else(|| SkypydbError::database("Inserted row is missing generated ID"))?
            .to_string();

        Ok(inserted_id)
    }

    pub fn search(
        &self,
        table_name: &str,
        index: Option<&str>,
        filters: &DataMap,
    ) -> Result<Vec<DataMap>> {
        let table_name = InputValidator::validate_table_name(table_name)?;
        let filters = InputValidator::validate_filter_map(filters)?;
        if !self.table_exists(&table_name) {
            return Err(SkypydbError::table_not_found(format!(
                "Table '{table_name}' not found"
            )));
        }
        let mut conditions = Vec::new();
        let mut params = Vec::<SqlValue>::new();
        if let Some(index_value) = index {
            let sanitized_index = InputValidator::sanitize_string(index_value);
            let columns = self.get_table_columns_names(&table_name)?;
            let non_standard_columns = columns
                .into_iter()
                .filter(|column| column != "id" && column != "created_at")
                .collect::<Vec<_>>();
            if !non_standard_columns.is_empty() {
                let mut index_conditions = Vec::new();

                for column in non_standard_columns {
                    index_conditions.push(format!("[{column}] = ?"));
                    params.push(SqlValue::Text(sanitized_index.clone()));
                }
                conditions.push(format!("({})", index_conditions.join(" OR ")));
            }
        }

        for (column, value) in &filters {
            match value {
                Value::Array(values) => {
                    if values.is_empty() {
                        return Err(SkypydbError::invalid_search(format!(
                            "Empty list provided for filter '{column}'"
                        )));
                    }
                    let placeholders = std::iter::repeat("?")
                        .take(values.len())
                        .collect::<Vec<_>>()
                        .join(", ");
                    conditions.push(format!("[{column}] IN ({placeholders})"));

                    for item in values {
                        params.push(json_value_to_filter_sql(item));
                    }
                }
                Value::Null => {
                    conditions.push(format!("[{column}] IS NULL"));
                }
                other => {
                    conditions.push(format!("[{column}] = ?"));
                    params.push(json_value_to_filter_sql(other));
                }
            }
        }
        let where_clause = if conditions.is_empty() {
            "1=1".to_string()
        } else {
            conditions.join(" AND ")
        };
        let query = format!("SELECT * FROM [{table_name}] WHERE {where_clause}");
        let conn = self.lock_connection()?;
        let mut statement = conn.prepare(&query)?;
        let column_names = statement
            .column_names()
            .iter()
            .map(|name| (*name).to_string())
            .collect::<Vec<_>>();
        let mut rows = statement.query(params_from_iter(params.iter()))?;
        let mut results = Vec::new();

        while let Some(row) = rows.next()? {
            let map = row_to_map(row, &column_names)?;
            results.push(self.decrypt_data(&map)?);
        }

        Ok(results)
    }

    pub fn delete_rows(&self, table_name: &str, filters: &DataMap) -> Result<usize> {
        let table_name = InputValidator::validate_table_name(table_name)?;
        let filters = InputValidator::validate_filter_map(filters)?;
        if !self.table_exists(&table_name) {
            return Err(SkypydbError::table_not_found(format!(
                "Table '{table_name}' not found"
            )));
        }
        if filters.is_empty() {
            return Err(SkypydbError::validation(
                "Cannot delete without filters. Use filters to specify which rows to delete.",
            ));
        }
        let mut conditions = Vec::new();
        let mut params = Vec::<SqlValue>::new();

        for (column, value) in &filters {
            match value {
                Value::Array(values) => {
                    if values.is_empty() {
                        return Err(SkypydbError::validation(format!(
                            "Empty list provided for filter '{column}'"
                        )));
                    }
                    let placeholders = std::iter::repeat("?")
                        .take(values.len())
                        .collect::<Vec<_>>()
                        .join(", ");
                    conditions.push(format!("[{column}] IN ({placeholders})"));

                    for item in values {
                        params.push(json_value_to_filter_sql(item));
                    }
                }
                Value::Null => {
                    conditions.push(format!("[{column}] IS NULL"));
                }
                other => {
                    conditions.push(format!("[{column}] = ?"));
                    params.push(json_value_to_filter_sql(other));
                }
            }
        }
        let where_clause = conditions.join(" AND ");
        let query = format!("DELETE FROM [{table_name}] WHERE {where_clause}");
        let conn = self.lock_connection()?;
        let affected_rows = conn.execute(&query, params_from_iter(params.iter()))?;

        Ok(affected_rows)
    }

    fn add_columns_if_needed(&self, table_name: &str, columns: &[String]) -> Result<()> {
        let table_name = InputValidator::validate_table_name(table_name)?;
        let existing_columns = self
            .get_table_columns_names(&table_name)?
            .into_iter()
            .collect::<HashSet<_>>();
        let conn = self.lock_connection()?;

        for column in columns {
            let validated_column = InputValidator::validate_column_name(column)?;
            if existing_columns.contains(&validated_column)
                || validated_column == "id"
                || validated_column == "created_at"
            {
                continue;
            }
            conn.execute(
                &format!("ALTER TABLE [{table_name}] ADD COLUMN [{validated_column}] TEXT"),
                [],
            )?;
        }

        Ok(())
    }

    fn table_def_to_config(&self, table_definition: &TableDefinition) -> Result<Value> {
        let mut config = Map::new();

        for (column_name, validator) in &table_definition.columns {
            let value = match validator {
                Validator::Optional(inner) => {
                    let mut descriptor = Map::new();
                    descriptor.insert(
                        "type".to_string(),
                        Value::String(map_validator_to_type_name(inner).to_string()),
                    );
                    descriptor.insert("optional".to_string(), Value::Bool(true));
                    Value::Object(descriptor)
                }
                other => Value::String(map_validator_to_type_name(other).to_string()),
            };
            config.insert(column_name.clone(), value);
        }

        if !table_definition.indexes.is_empty() {
            let mut indexes = Vec::new();

            for index in &table_definition.indexes {
                let mut descriptor = Map::new();
                descriptor.insert("name".to_string(), Value::String(index.name.clone()));
                descriptor.insert(
                    "fields".to_string(),
                    Value::Array(
                        index
                            .fields
                            .iter()
                            .map(|field| Value::String(field.clone()))
                            .collect(),
                    ),
                );
                indexes.push(Value::Object(descriptor));
            }
            config.insert("_indexes".to_string(), Value::Array(indexes));
        }

        Ok(Value::Object(config))
    }

    fn normalize_config(&self, config: &Value) -> Value {
        let Value::Object(config_object) = config else {
            return config.clone();
        };
        let mut normalized = Map::new();

        for (column_name, column_type) in config_object {
            match column_type {
                Value::Object(descriptor) => {
                    let mut normalized_descriptor = descriptor.clone();
                    let normalized_type = descriptor
                        .get("type")
                        .map(value_to_string)
                        .unwrap_or_else(|| "str".to_string());
                    normalized_descriptor
                        .insert("type".to_string(), Value::String(normalized_type));
                    normalized.insert(column_name.clone(), Value::Object(normalized_descriptor));
                }
                Value::Array(_) => {
                    normalized.insert(column_name.clone(), column_type.clone());
                }
                Value::String(_) => {
                    normalized.insert(column_name.clone(), column_type.clone());
                }
                _ => {
                    normalized.insert(
                        column_name.clone(),
                        Value::String(value_to_string(column_type)),
                    );
                }
            }
        }
        Value::Object(normalized)
    }

    fn encrypt_data(&self, data: &DataMap) -> Result<DataMap> {
        if !self.encryption_manager.enabled() {
            return Ok(data.clone());
        }
        let fields = self
            .encrypted_fields
            .iter()
            .filter(|field| data.contains_key(*field))
            .cloned()
            .collect::<Vec<_>>();

        self.encryption_manager
            .encrypt_map(data, Some(fields.as_slice()))
    }

    fn decrypt_data(&self, data: &DataMap) -> Result<DataMap> {
        if !self.encryption_manager.enabled() {
            return Ok(data.clone());
        }
        let fields = self
            .encrypted_fields
            .iter()
            .filter(|field| data.contains_key(*field))
            .cloned()
            .collect::<Vec<_>>();

        self.encryption_manager
            .decrypt_map(data, Some(fields.as_slice()))
    }
}

fn map_validator_to_type_name(validator: &Validator) -> &'static str {
    match validator {
        Validator::String => "str",
        Validator::Int64 => "int",
        Validator::Float64 => "float",
        Validator::Boolean => "bool",
        Validator::Optional(inner) => map_validator_to_type_name(inner),
    }
}

fn json_value_to_insert_sql(value: &Value) -> SqlValue {
    match value {
        Value::Null => SqlValue::Null,
        Value::String(text) => SqlValue::Text(text.clone()),
        _ => SqlValue::Text(value_to_string(value)),
    }
}

fn json_value_to_filter_sql(value: &Value) -> SqlValue {
    match value {
        Value::Null => SqlValue::Null,
        Value::String(text) => SqlValue::Text(text.clone()),
        _ => SqlValue::Text(value_to_string(value)),
    }
}

fn value_to_string(value: &Value) -> String {
    match value {
        Value::Null => "null".to_string(),
        Value::Bool(value) => value.to_string(),
        Value::Number(number) => number.to_string(),
        Value::String(text) => text.clone(),
        _ => value.to_string(),
    }
}

fn row_to_map(row: &rusqlite::Row<'_>, column_names: &[String]) -> Result<DataMap> {
    let mut map = Map::new();

    for (index, column_name) in column_names.iter().enumerate() {
        let value_ref = row.get_ref(index)?;
        map.insert(column_name.clone(), sql_ref_to_json(value_ref)?);
    }

    Ok(map)
}

fn sql_ref_to_json(value: ValueRef<'_>) -> Result<Value> {
    match value {
        ValueRef::Null => Ok(Value::Null),
        ValueRef::Integer(number) => Ok(Value::Number(Number::from(number))),
        ValueRef::Real(number) => Number::from_f64(number)
            .map(Value::Number)
            .ok_or_else(|| SkypydbError::database("Invalid REAL value from sqlite")),
        ValueRef::Text(bytes) => {
            let text = std::str::from_utf8(bytes)
                .map_err(|error| SkypydbError::database(format!("Invalid UTF-8 text: {error}")))?;
            Ok(Value::String(text.to_string()))
        }
        ValueRef::Blob(bytes) => Ok(Value::String(
            base64::engine::general_purpose::STANDARD.encode(bytes),
        )),
    }
}

#[cfg(test)]
mod tests {
    use super::{DataMap, ReactiveDatabase};
    use crate::columns;
    use crate::schema::{define_schema, define_table, value};
    use tempfile::tempdir;

    #[test]
    fn reactive_database_crud_roundtrip() {
        let dir = tempdir().expect("tempdir");
        let db_path = dir.path().join("reactive.db");
        let db = ReactiveDatabase::new(db_path.to_string_lossy(), None, None, None).expect("db");
        let table = define_table(columns! {
            "name" => value::string(),
            "age" => value::int64(),
        });
        let schema = define_schema(std::collections::BTreeMap::from([(
            "users".to_string(),
            table,
        )]));

        db.get_or_create_tables(&schema).expect("create tables");

        let mut row = DataMap::new();
        row.insert("name".to_string(), serde_json::json!("Ada"));
        row.insert("age".to_string(), serde_json::json!(31));
        let inserted_id = db.add_data("users", &row, true).expect("add");
        assert!(!inserted_id.is_empty());
        let results = db
            .search(
                "users",
                None,
                &DataMap::from_iter([(String::from("name"), serde_json::json!("Ada"))]),
            )
            .expect("search");
        assert_eq!(results.len(), 1);
        let deleted = db
            .delete_rows(
                "users",
                &DataMap::from_iter([(String::from("name"), serde_json::json!("Ada"))]),
            )
            .expect("delete");
        assert_eq!(deleted, 1);
    }
}
