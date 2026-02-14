//! Skypydb Rust library exports.

pub mod api;
pub mod cli;
pub mod database;
pub mod embeddings;
pub mod errors;
pub mod schema;
pub mod security;
pub mod server;
pub mod table;
pub use api::{Collection, ReactiveClient, VectorClient};
pub use database::database_linker::{DatabaseLinker, DatabaseType, DbLink, DiscoveredDbLink};
pub use database::reactive_database::{DataMap, ReactiveDatabase};
pub use database::vector_database::{
    CollectionInfo, VectorDatabase, VectorGetResult, VectorQueryResult,
};
pub use embeddings::{
    get_embedding_function, EmbeddingFunction, OllamaEmbedding, OpenAIEmbedding,
    SentenceTransformerEmbedding,
};
pub use errors::{Result, SkypydbError};
pub use schema::{define_schema, define_table, value, Schema, TableDefinition, Validator};
pub use security::{EncryptionManager, InputValidator};
pub use server::{build_router, run_dashboard_server, DashboardApi};
pub use table::Table;

#[macro_export]
macro_rules! columns {
    ($( $key:expr => $value:expr ),* $(,)?) => {{
        let mut map = ::std::collections::BTreeMap::new();
        $(
            map.insert($key.to_string(), $value);
        )*
        map
    }};
}

#[macro_export]
macro_rules! tables {
    ($( $key:expr => $value:expr ),* $(,)?) => {{
        let mut map = ::std::collections::BTreeMap::new();
        $(
            map.insert($key.to_string(), $value);
        )*
        map
    }};
}

#[macro_export]
macro_rules! json_map {
    ($( $key:expr => $value:expr ),* $(,)?) => {{
        let mut map = ::serde_json::Map::new();
        $(
            map.insert($key.to_string(), ::serde_json::json!($value));
        )*
        map
    }};
}
