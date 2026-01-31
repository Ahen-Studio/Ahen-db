<div align="center">
 <img alt="Skypy" width="auto" height="auto" src="https://github.com/amichyrpi/skypy-db/blob/main/docs/logo/dark.svg#gh-light-mode-only">
 <img alt="Skypy" width="auto" height="auto" src="https://github.com/amichyrpi/skypy-db/blob/main/docs/logo/dark.svg#gh-dark-mode-only">
</div>

<p align="center">
    <b>Skypy - Open Source Reactive and Vector Embeddings Database</b>. <br />
    The better way to build Python logging system! And adding memory to a LLM.
</p>

<div align="center">

![GitHub top language](https://img.shields.io/github/languages/top/Ahen-Studio/skypy-db)
[![PyPI](https://img.shields.io/pypi/v/skypydb.svg)](https://pypi.org/project/skypydb/)
[![License](https://img.shields.io/github/license/Ahen-Studio/skypy-db)](https://github.com/Ahen-Studio/skypy-db/blob/main/LICENSE)
[![CodeRabbit Reviews](https://img.shields.io/coderabbit/prs/github/Ahen-Studio/skypy-db?utm_source=oss&utm_medium=github&utm_campaign=Ahen-Studio%2Fskypy-db&labelColor=171717&color=FF570A&link=https%3A%2F%2Fcoderabbit.ai&label=CodeRabbit+Reviews)](https://coderabbit.ai)
[![Docs](https://img.shields.io/badge/Docs-blue.svg)](https://ahen.mintlify.app/)

</div>

```bash
pip install skypydb # python client
# or download from the source
# git clone https://github.com/Ahen-Studio/skypy-db.git
# cd skypy-db
# pip install -r requirements.txt
```

## Features

- Simple: fully-documented

- Table: create, delete, search data from tables

- Vector embeddings: create, search and delete vectors collections. It supports [ollama](https://ollama.com/download) embedding model (default model is [mxbai-embed-large](https://ollama.com/library/mxbai-embed-large)).

- Memory: add memory to a LLM by using [mem0](https://github.com/mem0ai/mem0) and our integration.

- Security, Input Validation: AES-256-GCM encryption for data at rest with selective field encryption, automatic protection against SQL injection attacks

- CLI: command line interface to initialize your database and launch the dashboard with one simple command

- Observable: Dashboard with real-time data, metrics, and query inspection

- Free & Open Source: MIT Licensed

## TODO

- [ ] Create the dashboard using Reflex
- [ ] update the documentation
- [ ] refactor /api/collection.py and /db/vector_database.py

## What's next!

- give us ideas!

## Cli

- use the cli to initialize your database and launch the dashboard with one simple command

```bash
skypydb dev
```

- run this command in your terminal

## API

- Use the API to interact with your database; before doing so, make sure to create a schema to define your tables.

```python
"""
Schema definition for Skypydb database tables.
This file defines all tables, their columns, types, and indexes.
"""

from skypydb.schema import defineSchema, defineTable
from skypydb.schema.values import v

# Define the schema with all tables
schema = defineSchema({
    
    # Table pour les logs de succès
    "success": defineTable({
        "component": v.string(),
        "action": v.string(),
        "message": v.string(),
        "details": v.optional(v.string()),
        "user_id": v.optional(v.string()),
    })
    .index("by_component", ["component"])
    .index("by_action", ["action"])
    .index("by_user", ["user_id"])
    .index("by_component_and_action", ["component", "action"]),

    # Table pour les logs d'avertissement
    "warning": defineTable({
        "component": v.string(),
        "action": v.string(),
        "message": v.string(),
        "details": v.optional(v.string()),
        "user_id": v.optional(v.string()),
    })
    .index("by_component", ["component"])
    .index("by_action", ["action"])
    .index("by_user", ["user_id"])
    .index("by_component_and_action", ["component", "action"]),

    # Table pour les logs d'erreur
    "error": defineTable({
        "component": v.string(),
        "action": v.string(),
        "message": v.string(),
        "details": v.optional(v.string()),
        "user_id": v.optional(v.string()),
    })
    .index("by_component", ["component"])
    .index("by_action", ["action"])
    .index("by_user", ["user_id"])
    .index("by_component_and_action", ["component", "action"]),
})
```

- after creating the schema file containing the tables, you can add data to your database

```python
import skypydb

# Create a client
client = skypydb.Client()

# Create tables from the schema
# This reads the schema from skypydb/schema.py and creates all tables
tables = client.create_table()

# Access your tables
success_table = tables["success"]
warning_table = tables["warning"]
error_table = tables["error"]

# Insert data
# Insert success logs
success_table.add(
    component="AuthService",
    action="login",
    message="User logged in successfully",
    user_id="user123"
)

# Insert warning logs
warning_table.add(
    component="AuthService",
    action="login_attempt",
    message="Multiple failed login attempts",
    user_id="user456",
    details="5 failed attempts in 5 minutes"
)

# Insert error logs
error_table.add(
    component="DatabaseService",
    action="connection",
    message="Connection timeout",
    user_id="system",
    details="Timeout after 30 seconds"
)
```

- after adding data to your database you can search specific data using the search method

```python
user_success_logs = success_table.search(
    index="by_user",
    user_id="user123"
)
for user_success_log in user_success_logs:
    print(user_success_log)
```

- you can also delete specific data from your database using the delete method

```python
success_table.delete(
    component="AuthService",
    user_id="user123"
)
```

### Vector

- Use the vector API to perform vector operations on your database, it is useful for adding memory to an LLM.

```python
from skypydb import Vector_Client

# Create a client
client = Vector_Client()

# Create a collection
collection = client.create_collection("my-documents")

# Add documents (automatically embedded using Ollama)
collection.add(
    documents=["This is document1", "This is document2"],
    metadatas=[{"source": "notion"}, {"source": "google-docs"}],
    ids=["doc1", "doc2"]
)

# Query for similar documents
results = collection.query(
    query_texts=["This is a query document"],
    n_results=2
)

# Access results
for i, doc_id in enumerate(results["ids"][0]):
    print(f"ID: {doc_id}")
    print(f"Document: {results['documents'][0][i]}")
    print(f"Distance: {results['distances'][0][i]}")
```

### Mem0

- use this command to install skypydb and mem0

```bash
pip install skypydb[mem0]
```

- to use mem0 with skypydb you will need to download the mem0 folder and place every file in the correct directory in the mem0 installation folder.

```python
from mem0 import Memory

# Local mem0 config
config = {
    "vector_store": {
        "provider": "skypydb",
        "config": {
            "collection_name": "memory",
            "path": "db/_generated/mem0_vector.db"
        }
    },
    "llm": {
        "provider": "ollama",
        "config": {
            "model": "llama3.1:latest",
            "temperature": 0.3,
            "max_tokens": 1024,
            "ollama_base_url": "http://localhost:11434",
        },
    },
    "embedder": {
        "provider": "ollama",
        "config": {
            "model": "mxbai-embed-large"
        }
    }
}

m = Memory.from_config(config)

# Add memories
m.add("I love Python programming", user_id="user1")
m.add("My favorite color is blue", user_id="user1")

# Search memories
results = m.search("What programming language do I like?", user_id="user1")

print(results)
```

### Secure Implementation

- first create an encryption key and a salt key and make them available in the .env.local file don't show those keys to anyone, you can use the Cli to generate those keys

```python
# you can generate a secure encryption key and salt using the cli
# or generate a secure encryption key and salt using the this example code

from skypydb.security import EncryptionManager

# Generate a secure encryption key
encryption_key = EncryptionManager.generate_key()
salt = EncryptionManager.generate_salt()
print(encryption_key) # don't show this key to anyone
print(salt) # don't show this salt to anyone
```

- Use the encryption key to encrypt sensitive data

```python
import os
import skypydb
from dotenv import load_dotenv

# Load environment variables from .env file
load_dotenv()

# Load encryption key from environment
encryption_key = os.getenv("ENCRYPTION_KEY") # create a encryption key and make it available in .env file before using it, don't show this key to anyone
salt_key = os.getenv("SALT_KEY") # create a salt key and make it available in .env file before using it, don't show this salt to anyone

# transform salt key to bytes
if salt_key is None:
    raise ValueError("SALT_KEY missing")
salt_bytes = salt_key.encode("utf-8")

# Create encrypted database
client = skypydb.Client(
    encryption_key=encryption_key,
    salt=salt_bytes,
    encrypted_fields=["message"]  # Optional: encrypt only sensitive fields
)

# All operations work the same - encryption is transparent!
tables = client.create_table()

# Access your tables
success_table = tables["success"]
warning_table = tables["warning"]
error_table = tables["error"]

# Automatically encrypted
success_table.add(
    component="AuthService",
    action="login",
    message="User logged in successfully", # only this field is encrypted if encrypted_fields is not None
    user_id="user123"
)

# Data is automatically decrypted when retrieved
user_success_logs = success_table.search(
    index="by_user",
    user_id="user123"
)
for user_success_log in user_success_logs:
    print(user_success_log)
```

Learn more on our [Docs](https://ahen.mintlify.app/)

## All Thanks To Our Contributors:

<a href="https://github.com/Ahen-Studio/skypy-db/graphs/contributors">
  <img src="https://contrib.rocks/image?repo=Ahen-Studio/skypy-db" />
</a>

## License

[MIT](./LICENSE)
