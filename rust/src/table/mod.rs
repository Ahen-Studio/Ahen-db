//! Table-level operations for reactive databases.

use crate::database::reactive_database::{DataMap, ReactiveDatabase};
use crate::errors::{Result, SkypydbError};
use serde_json::{Map, Value};

#[derive(Clone)]
pub struct Table {
    db: ReactiveDatabase,
    table_name: String,
}

impl Table {
    pub fn new(db: ReactiveDatabase, table_name: impl Into<String>) -> Result<Self> {
        let table_name = table_name.into();
        if !db.table_exists(&table_name) {
            return Err(SkypydbError::table_not_found(format!(
                "Table '{table_name}' not found"
            )));
        }

        Ok(Self { db, table_name })
    }

    pub fn name(&self) -> &str {
        &self.table_name
    }

    pub fn add(&self, mut data: DataMap) -> Result<Vec<String>> {
        if let Some(id_value) = data.get("id") {
            let auto_scalar = id_value.as_str() == Some("auto");
            let auto_list = id_value
                .as_array()
                .map(|values| values.len() == 1 && values[0].as_str() == Some("auto"))
                .unwrap_or(false);
            if auto_scalar || auto_list {
                data.remove("id");
            }
        }
        let mut max_length = 1_usize;

        for (key, value) in &data {
            if let Value::Array(values) = value {
                if values.is_empty() {
                    return Err(SkypydbError::validation(format!(
                        "Empty list provided for '{key}'"
                    )));
                }
                max_length = max_length.max(values.len());
            }
        }
        let mut inserted_ids = Vec::new();

        for row_index in 0..max_length {
            let mut row_data = Map::new();

            for (key, value) in &data {
                match value {
                    Value::Array(values) => {
                        let selected = values
                            .get(row_index)
                            .or_else(|| values.last())
                            .cloned()
                            .unwrap_or(Value::Null);
                        row_data.insert(key.clone(), selected);
                    }
                    other => {
                        row_data.insert(key.clone(), other.clone());
                    }
                }
            }
            let validated_data = self
                .db
                .validate_data_with_config(&self.table_name, &row_data)?;
            let row_id = self.db.add_data(&self.table_name, &validated_data, true)?;
            inserted_ids.push(row_id);
        }

        Ok(inserted_ids)
    }

    pub fn search(&self, index: Option<&str>, filters: DataMap) -> Result<Vec<DataMap>> {
        self.db.search(&self.table_name, index, &filters)
    }

    pub fn delete(&self, filters: DataMap) -> Result<usize> {
        self.db.delete_rows(&self.table_name, &filters)
    }

    pub fn get_all(&self) -> Result<Vec<DataMap>> {
        self.db.get_all_data(&self.table_name)
    }

    pub fn database(&self) -> &ReactiveDatabase {
        &self.db
    }
}
