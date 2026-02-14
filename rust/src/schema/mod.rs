//! Schema builders and validators for reactive tables.

use crate::errors::{Result, SkypydbError};
use serde_json::{Map, Value};
use std::collections::BTreeMap;

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum Validator {
    String,
    Int64,
    Float64,
    Boolean,
    Optional(Box<Validator>),
}

impl Validator {
    pub fn validate(&self, value: &Value) -> bool {
        match self {
            Validator::String => value.is_string(),
            Validator::Int64 => value.as_i64().is_some(),
            Validator::Float64 => value.as_f64().is_some(),
            Validator::Boolean => value.is_boolean(),
            Validator::Optional(inner) => value.is_null() || inner.validate(value),
        }
    }

    pub fn is_optional(&self) -> bool {
        matches!(self, Validator::Optional(_))
    }

    pub fn base(&self) -> &Validator {
        match self {
            Validator::Optional(inner) => inner,
            other => other,
        }
    }

    pub fn sql_type(&self) -> &'static str {
        match self.base() {
            Validator::Int64 | Validator::Boolean => "INTEGER",
            Validator::Float64 => "REAL",
            _ => "TEXT",
        }
    }

    pub fn repr(&self) -> String {
        match self {
            Validator::String => "value.string()".to_string(),
            Validator::Int64 => "value.int64()".to_string(),
            Validator::Float64 => "value.float64()".to_string(),
            Validator::Boolean => "value.boolean()".to_string(),
            Validator::Optional(inner) => format!("value.optional({})", inner.repr()),
        }
    }
}

pub mod value {
    use super::Validator;

    pub fn string() -> Validator {
        Validator::String
    }

    pub fn int64() -> Validator {
        Validator::Int64
    }

    pub fn float64() -> Validator {
        Validator::Float64
    }

    pub fn boolean() -> Validator {
        Validator::Boolean
    }

    pub fn optional(validator: Validator) -> Validator {
        Validator::Optional(Box::new(validator))
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct IndexDefinition {
    pub name: String,
    pub fields: Vec<String>,
}

#[derive(Clone, Debug, Default)]
pub struct TableDefinition {
    pub columns: BTreeMap<String, Validator>,
    pub indexes: Vec<IndexDefinition>,
    pub table_name: Option<String>,
}

impl TableDefinition {
    pub fn new(columns: BTreeMap<String, Validator>) -> Self {
        Self {
            columns,
            indexes: Vec::new(),
            table_name: None,
        }
    }

    pub fn index(mut self, name: impl Into<String>, fields: Vec<&str>) -> Result<Self> {
        let name = name.into();

        for field in &fields {
            if !self.columns.contains_key(*field) {
                return Err(SkypydbError::validation(format!(
                    "Cannot create index '{name}' on non-existent field '{field}'. Available fields: {:?}",
                    self.columns.keys().collect::<Vec<_>>()
                )));
            }
        }
        self.indexes.push(IndexDefinition {
            name,
            fields: fields.into_iter().map(|value| value.to_string()).collect(),
        });

        Ok(self)
    }

    pub fn validate_row(&self, row_data: &Map<String, Value>) -> Result<()> {
        for (column_name, validator) in &self.columns {
            match row_data.get(column_name) {
                Some(value) => {
                    if !validator.validate(value) {
                        return Err(SkypydbError::validation(format!(
                            "Invalid value for column '{column_name}': expected {}, got {value}",
                            validator.repr(),
                        )));
                    }
                }
                None => {
                    if !validator.is_optional() {
                        return Err(SkypydbError::validation(format!(
                            "Missing required column: '{column_name}'"
                        )));
                    }
                }
            }
        }

        Ok(())
    }

    pub fn get_sql_columns(&self) -> Vec<String> {
        let mut sql_columns = vec![
            "id TEXT PRIMARY KEY".to_string(),
            "created_at TEXT NOT NULL".to_string(),
        ];

        for (column_name, validator) in &self.columns {
            if column_name == "id" || column_name == "created_at" {
                continue;
            }
            sql_columns.push(format!("[{column_name}] {}", validator.sql_type()));
        }
        sql_columns
    }

    pub fn get_sql_indexes(&self) -> Vec<String> {
        let Some(table_name) = &self.table_name else {
            return Vec::new();
        };

        self.indexes
            .iter()
            .map(|index| {
                let index_name = format!("idx_{table_name}_{}", index.name);
                let fields = index
                    .fields
                    .iter()
                    .map(|field| format!("[{field}]"))
                    .collect::<Vec<_>>()
                    .join(", ");
                format!("CREATE INDEX IF NOT EXISTS [{index_name}] ON [{table_name}] ({fields})")
            })
            .collect()
    }
}

#[derive(Clone, Debug, Default)]
pub struct Schema {
    pub tables: BTreeMap<String, TableDefinition>,
}

impl Schema {
    pub fn new(tables: BTreeMap<String, TableDefinition>) -> Self {
        Self { tables }
    }

    pub fn get_table_definition(&self, table_name: &str) -> Option<&TableDefinition> {
        self.tables.get(table_name)
    }

    pub fn get_all_table_names(&self) -> Vec<String> {
        self.tables.keys().cloned().collect()
    }
}

pub fn define_table(columns: BTreeMap<String, Validator>) -> TableDefinition {
    TableDefinition::new(columns)
}

pub fn define_schema(mut tables: BTreeMap<String, TableDefinition>) -> Schema {
    for (table_name, table_definition) in tables.iter_mut() {
        table_definition.table_name = Some(table_name.clone());
    }
    Schema::new(tables)
}
