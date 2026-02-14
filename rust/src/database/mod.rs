//! Database internals for Skypydb.

pub mod database_linker;
pub mod reactive_database;
pub mod vector_database;
pub use crate::errors::{Result, SkypydbError};
pub use database_linker::{DatabaseLinker, DatabaseType, DbLink, DiscoveredDbLink};
pub use reactive_database::{DataMap, ReactiveDatabase};
pub use vector_database::{CollectionInfo, VectorDatabase, VectorGetResult, VectorQueryResult};
