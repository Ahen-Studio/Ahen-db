<div align="center">
 <img alt="Skypydb" width="auto" height="auto" src="https://github.com/Ahen-Studio/skypydb/blob/main/docs/logo/dark.svg#gh-light-mode-only">
 <img alt="Skypydb" width="auto" height="auto" src="https://github.com/Ahen-Studio/skypydb/blob/main/docs/logo/dark.svg#gh-dark-mode-only">
</div>

<p align="center">
    <b>Skypydb - Open Source Reactive and Vector Embeddings Database</b>. <br />
    Rust client for Skypydb.
</p>

<div align="center">

![GitHub commit activity](https://img.shields.io/github/commit-activity/m/Ahen-Studio/skypydb)
[![Crates.io](https://img.shields.io/crates/v/skypydb)](https://crates.io/crates/skypydb)
[![PyPI](https://img.shields.io/pypi/v/skypydb.svg)](https://pypi.org/project/skypydb/)
[![License](https://img.shields.io/badge/License-MIT-green.svg)](https://github.com/Ahen-Studio/skypydb/blob/main/LICENSE)
[![Docs](https://img.shields.io/badge/Docs-blue.svg)](https://ahen.mintlify.app/)

</div>

```bash
cargo add skypydb # rust client
# or download from the source
# git clone https://github.com/Ahen-Studio/skypydb.git
```

## Features

- Simple: fully-documented and easy to debug with detailed error messages

- Table: create, delete, search data from tables

- Vector embeddings: create, search and delete vectors collections. It supports [Ollama](https://ollama.com/download), [OpenAI](https://developers.openai.com/api/docs/guides/embeddings), [Sentence-Transformers](https://huggingface.co/sentence-transformers) embeddings models (default model is [mxbai-embed-large](https://ollama.com/library/mxbai-embed-large) from Ollama).

- Memory: add memory to a LLM by using [mem0](https://github.com/mem0ai/mem0) and our integration.

- Security, Input Validation: AES-256-GCM encryption for data at rest with selective field encryption, automatic protection against SQL injection attacks

- CLI: command line interface to initialize your database and launch the dashboard with one simple command

- Observable: Dashboard with real-time data, metrics, and query inspection

- Free & Open Source: MIT Licensed

- Cross-platform: Windows, Linux, MacOS

## What's next!

- give us ideas!

## Error Codes

- Skypydb uses standardized error codes to help you quickly identify and handle issues:

| Code       | Error                        | Description                                                             |
|------------|------------------------------|-------------------------------------------------------------------------|
| **SKY001** | SkypydbError                 | Base exception for all Skypydb errors                                   |
| **SKY101** | TableNotFoundError           | Raised when attempting to access a table that doesn't exist             |
| **SKY102** | TableAlreadyExistsError      | Raised when trying to create a table that already exists                |
| **SKY103** | DatabaseError                | Raised when a database operation fails                                  |
| **SKY201** | InvalidSearchError           | Raised when search parameters are invalid                               |
| **SKY301** | SecurityError                | Raised when a security operation fails                                  |
| **SKY302** | ValidationError              | Raised when input validation fails                                      |
| **SKY303** | EncryptionError              | Raised when encryption/decryption operations fail                       |
| **SKY401** | CollectionNotFoundError      | Raised when attempting to access a vector collection that doesn't exist |
| **SKY402** | CollectionAlreadyExistsError | Raised when trying to create a collection that already exists           |
| **SKY403** | EmbeddingError               | Raised when embedding generation fails                                  |
| **SKY404** | VectorSearchError            | Raised when vector similarity search fails                              |

## Cli

- use the cli to initialize your database and launch the dashboard with one simple command

```bash
cargo run -p skypydb --bin skypydbrust dev # interactive menu to initialize your database and start the dashboard
# or use the main commands to initialize your database and launch the dashboard
cargo run -p skypydb --bin skypydbrust init
cargo run -p skypydb --bin skypydbrust keygen
cargo run -p skypydb --bin skypydbrust dashboard
```

- run these commands in your terminal

## API

- Use the API to interact with your database; before doing so, make sure to create a schema to define your tables.

```rust
use skypydb::schema::value;
use skypydb::{columns, define_schema, define_table, tables, Result, Schema};

pub fn build_schema() -> Result<Schema> {
    let success = define_table(columns! {
        "component" => value::string(),
        "action" => value::string(),
        "message" => value::string(),
        "details" => value::optional(value::string()),
        "user_id" => value::optional(value::string()),
    })
    .index("by_component", vec!["component"])?
    .index("by_action", vec!["action"])?
    .index("by_user", vec!["user_id"])?
    .index("by_component_and_action", vec!["component", "action"])?;

    let warning = define_table(columns! {
        "component" => value::string(),
        "action" => value::string(),
        "message" => value::string(),
        "details" => value::optional(value::string()),
        "user_id" => value::optional(value::string()),
    })
    .index("by_component", vec!["component"])?
    .index("by_action", vec!["action"])?
    .index("by_user", vec!["user_id"])?
    .index("by_component_and_action", vec!["component", "action"])?;

    let error = define_table(columns! {
        "component" => value::string(),
        "action" => value::string(),
        "message" => value::string(),
        "details" => value::optional(value::string()),
        "user_id" => value::optional(value::string()),
    })
    .index("by_component", vec!["component"])?
    .index("by_action", vec!["action"])?
    .index("by_user", vec!["user_id"])?
    .index("by_component_and_action", vec!["component", "action"])?;

    Ok(define_schema(tables! {
        "success" => success,
        "warning" => warning,
        "error" => error,
    }))
}
```

- after creating the schema file containing the tables, you can add data to your database

```rust
#[path = "schema.rs"]
//Load the schema file
mod schema;

use skypydb::{json_map, ReactiveClient, Result};

fn main() -> Result<()> {
    //Create a new client
    let client = ReactiveClient::new("./db/_generated/skypydb_rust.db", None, None, None)?;
    let schema = schema::build_schema()?;

    //Get or create tables from the schema
    let tables = client.get_or_create_tables(&schema)?;

    //Error handling for tables
    let success_table = tables
        .get("success")
        .ok_or_else(|| skypydb::SkypydbError::table_not_found("Table 'success' not found"))?;
    let warning_table = tables
        .get("warning")
        .ok_or_else(|| skypydb::SkypydbError::table_not_found("Table 'warning' not found"))?;
    let error_table = tables
        .get("error")
        .ok_or_else(|| skypydb::SkypydbError::table_not_found("Table 'error' not found"))?;

    //Add data to the tables
    let success_ids = success_table.add(json_map! {
        "component" => "AuthService",
        "action" => "login",
        "message" => "User logged in successfully",
        "user_id" => "user123",
    })?;

    let warning_ids = warning_table.add(json_map! {
        "component" => "AuthService",
        "action" => "login_attempt",
        "message" => "Multiple failed login attempts",
        "user_id" => "user456",
        "details" => "5 failed attempts in 5 minutes",
    })?;

    let error_ids = error_table.add(json_map! {
        "component" => "DatabaseService",
        "action" => "connection",
        "message" => "Connection timeout",
        "user_id" => "system",
        "details" => "Timeout after 30 seconds",
    })?;

    Ok(())
}
```

- after adding data to your database you can search specific data using the search method

```rust
#[path = "schema.rs"]
//Load the schema file
mod schema;

use skypydb::{json_map, ReactiveClient, Result};

fn main() -> Result<()> {
    //Adding fonction show earlier

    //Search for data in the table
    let inserted_success = success_table.search(
        None,
        json_map! {
            "id" => success_ids.clone(),
        },
    )?;
    let inserted_warning = warning_table.search(
        None,
        json_map! {
            "id" => warning_ids.clone(),
        },
    )?;
    let inserted_error = error_table.search(
        None,
        json_map! {
            "id" => error_ids.clone(),
        },
    )?;

    //Print the results
    for row in inserted_success {
        println!("  [success] {row:?}");
    }
    for row in inserted_warning {
        println!("  [warning] {row:?}");
    }
    for row in inserted_error {
        println!("  [error]   {row:?}");
    }

    Ok(())
}
```

- you can also delete specific data from your database using the delete method

```rust
fn main() -> Result<()> {
    //Adding fonction show earlier

    //Delete data from the table
    let deleted = success_table.delete(json_map! {
        "component" => "AuthService",
        "user_id" => "user123",
    })?;

    //Print the results
    if deleted.is_empty() {
        println!("No deleted data found.");
    } else {
        for item in deleted {
            println!("{item}");
        }
    }

    Ok(())
}
```

### Vector

- Use the vector API to perform vector operations on your database, it is useful for adding memory to an LLM.

```rust
use serde_json::json;
use skypydb::{json_map, Result, VectorClient};

fn main() -> Result<()> {
    //Create a new vector client with ollama
    let client = VectorClient::new(
        "./db/_generated/vector_rust_ollama.db",
        "ollama",
        Some(json_map! {
            "model" => "mxbai-embed-large",
            "base_url" => "http://localhost:11434",
        }),
    )?;

    //Get or create collection
    let collection = client.get_or_create_collection("my-documents", None)?;

    //Add documents to the collection
    collection.add(
        vec!["doc1".to_string(), "doc2".to_string()],
        None,
        Some(vec![
            "This is document1".to_string(),
            "This is document2".to_string(),
        ]),
        Some(vec![
            json!({"source": "notion"}),
            json!({"source": "google-docs"}),
        ]),
    )?;

    //Query the collection
    let results = collection.query(
        None,
        Some(vec!["This is a query document".to_string()]),
        2,
        None,
        None,
        None,
    )?;

    //Print the results
    if results
        .ids
        .first()
        .map(|ids| ids.is_empty())
        .unwrap_or(true)
    {
        println!("No results found.");
    } else {
        for (index, id) in results.ids[0].iter().enumerate() {
            let document = results
                .documents
                .as_ref()
                .and_then(|docs| docs.first())
                .and_then(|docs| docs.get(index))
                .and_then(|doc| doc.as_ref())
                .cloned()
                .unwrap_or_default();
            let distance = results
                .distances
                .as_ref()
                .and_then(|distances| distances.first())
                .and_then(|distances| distances.get(index))
                .copied()
                .unwrap_or_default();

            println!("{id}, {document}, {distance}");
        }
    }

    Ok(())
}
```

- use the vector API with OpenAI

```rust
use serde_json::json;
use skypydb::{json_map, Result, VectorClient};

fn main() -> Result<()> {
    // Set your OpenAI API key as an environment variable before running this example.
    let api_key = match std::env::var("OPENAI_API_KEY") {
        Ok(value) => value,
        Err(_) => {
            println!("Set OPENAI_API_KEY before running this example.");
            return Ok(());
        }
    };

    // Create a new vector client with OpenAI
    let client = VectorClient::new(
        "./db/_generated/vector_rust_openai.db",
        "openai",
        Some(json_map! {
            "api_key" => api_key,
            "model" => "text-embedding-3-small",
        }),
    )?;

    // Get or create collection
    let collection = client.get_or_create_collection("my-documents", None)?;

    // Add documents to the collection
    collection.add(
        vec!["doc1".to_string(), "doc2".to_string()],
        None,
        Some(vec![
            "This is document1".to_string(),
            "This is document2".to_string(),
        ]),
        Some(vec![
            json!({"source": "notion"}),
            json!({"source": "google-docs"}),
        ]),
    )?;

    // Query the collection
    let results = collection.query(
        None,
        Some(vec!["This is a query document".to_string()]),
        2,
        None,
        None,
        None,
    )?;

    // Print the results
    if results
        .ids
        .first()
        .map(|ids| ids.is_empty())
        .unwrap_or(true)
    {
        println!("No results found.");
    } else {
        for (index, id) in results.ids[0].iter().enumerate() {
            let document = results
                .documents
                .as_ref()
                .and_then(|docs| docs.first())
                .and_then(|docs| docs.get(index))
                .and_then(|doc| doc.as_ref())
                .cloned()
                .unwrap_or_default();
            let distance = results
                .distances
                .as_ref()
                .and_then(|distances| distances.first())
                .and_then(|distances| distances.get(index))
                .copied()
                .unwrap_or_default();

            println!("{id}, {document}, {distance}");
        }
    }

    Ok(())
}
```

- Use the vector API with Sentence transformers

```rust
use serde_json::json;
use skypydb::{json_map, Result, VectorClient};

fn main() -> Result<()> {
    // Create a new vector client with sentence transformers
    let client = VectorClient::new(
        "./db/_generated/vector_rust_sentence_transformers.db",
        "sentence-transformers",
        Some(json_map! {
            "model" => "all-MiniLM-L6-v2",
        }),
    )?;

    // Get or create collection
    let collection = client.get_or_create_collection("my-documents", None)?;

    // Add documents to the collection
    collection.add(
        vec!["doc1".to_string(), "doc2".to_string()],
        None,
        Some(vec![
            "This is document1".to_string(),
            "This is document2".to_string(),
        ]),
        Some(vec![
            json!({"source": "notion"}),
            json!({"source": "google-docs"}),
        ]),
    )?;

    // Query the collection
    let results = collection.query(
        None,
        Some(vec!["This is a query document".to_string()]),
        2,
        None,
        None,
        None,
    )?;

    // Print the results
    if results
        .ids
        .first()
        .map(|ids| ids.is_empty())
        .unwrap_or(true)
    {
        println!("No results found.");
    } else {
        for (index, id) in results.ids[0].iter().enumerate() {
            let document = results
                .documents
                .as_ref()
                .and_then(|docs| docs.first())
                .and_then(|docs| docs.get(index))
                .and_then(|doc| doc.as_ref())
                .cloned()
                .unwrap_or_default();
            let distance = results
                .distances
                .as_ref()
                .and_then(|distances| distances.first())
                .and_then(|distances| distances.get(index))
                .copied()
                .unwrap_or_default();

            println!("{id}, {document}, {distance}");
        }
    }

    Ok(())
}
```

### Secure Implementation

- first create an encryption key and a salt key and make them available in the .env.local file don't show those keys to anyone, you can use the Cli to generate those keys

```rust
// you can generate a secure encryption key and salt using the cli
// or generate a secure encryption key and salt using the this example code
use base64::engine::general_purpose::STANDARD as BASE64_STANDARD;
use base64::Engine;
use skypydb::{EncryptionManager, Result};

fn main() -> Result<()> {
    let key = EncryptionManager::generate_key();
    let salt = EncryptionManager::generate_salt(32)?;

    println!("ENCRYPTION_KEY={key}");
    println!("SALT_KEY={}", BASE64_STANDARD.encode(salt));

    Ok(())
}
```

- Use the encryption key to encrypt sensitive data

```rust
#[path = "../basic_implementation/schema.rs"]
//Load the schema file
mod schema;

use base64::engine::general_purpose::STANDARD as BASE64_STANDARD;
use base64::Engine;
use dotenv::from_filename;
use skypydb::{json_map, ReactiveClient, Result, SkypydbError};

fn main() -> Result<()> {
    // Load environment variables from .env.local file
    let _ = from_filename(".env.local");

    // Load encryption key from environment. Fail if not found
    let encryption_key = std::env::var("ENCRYPTION_KEY")
        .ok_or_else(|| {
            SkypydbError::validation("Missing ENCRYPTION_KEY. Set it in environment or .env.local.")
        })?;

    // Load salt key from environment. Fail if not found
    let salt_encoded = std::env::var("SALT_KEY")
        .ok_or_else(|| {
            SkypydbError::validation("Missing SALT_KEY. Set it in environment or .env.local.")
        })?;

    // Decode salt from base64
    let salt = BASE64_STANDARD
        .decode(salt_encoded)
        .map_err(|error| SkypydbError::validation(format!("Invalid SALT_KEY: {error}")))?;

    //Create a new client with encryption field set
    let client = ReactiveClient::new(
        "./db/_generated/skypydb_secure_rust.db",
        Some(encryption_key),
        Some(salt),
        Some(vec!["message".to_string()]),
    )?;
    let schema = schema::build_schema()?;

    //Get or create tables from the schema
    let tables = client.get_or_create_tables(&schema)?;

    //Error handling for tables
    let success_table = tables
        .get("success")
        .ok_or_else(|| skypydb::SkypydbError::table_not_found("Table 'success' not found"))?;

    //Add data to the tables
    let inserted = success_table.add(json_map! {
        "component" => "AuthService",
        "action" => "login",
        "message" => "User logged in successfully",
        "user_id" => "user123",
    })?;
    println!("Inserted: {inserted:?}");

    //Search for data
    let results = success_table.search(
        None,
        json_map! {
            "user_id" => "user123",
        },
    )?;

    //Print results
    println!("Decrypted results:");
    for result in results {
        println!("{result}");
    }

    Ok(())
}
```

Learn more on our [Docs](https://ahen.mintlify.app/)

## All Thanks To Our Contributors:

<a href="https://github.com/Ahen-Studio/skypydb/graphs/contributors">
  <img src="https://contrib.rocks/image?repo=Ahen-Studio/skypydb" />
</a>

## License

[MIT](./LICENSE)
