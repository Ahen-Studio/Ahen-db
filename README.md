<h1><div align="center">
 <img alt="Skypy" width="auto" height="auto" src="https://github.com/amichyrpi/skypy-db/blob/main/docs/logo/dark.svg#gh-light-mode-only">
 <img alt="Skypy" width="auto" height="auto" src="https://github.com/amichyrpi/skypy-db/blob/main/docs/logo/dark.svg#gh-dark-mode-only">
</div></h1>

<p align="center">
    <b>Skypy - open-source reactive database</b>. <br />
    The better way to build Python logging system!
</p>

<p align="center">
  <a href="https://pypi.org/project/skypydb" target="_blank">
      <img src="https://img.shields.io/pypi/v/skypydb" alt="PyPI">
  </a> |
  <a href="https://github.com/Ahen-Studio/skypy-db/blob/main/LICENSE" target="_blank">
      <img src="https://img.shields.io/badge/License-MIT-green.svg" alt="License">
  </a> |
  <a>
      <img src="https://img.shields.io/coderabbit/prs/github/Ahen-Studio/skypy-db?utm_source=oss&utm_medium=github&utm_campaign=Ahen-Studio%2Fskypy-db&labelColor=171717&color=FF570A&link=https%3A%2F%2Fcoderabbit.ai&label=CodeRabbit+Reviews"
  </a> |
  <a href="https://ahen.mintlify.app/" target="_blank">
      Docs
  </a>
</p>

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

- Security, Input Validation: AES-256-GCM encryption for data at rest with selective field encryption, automatic protection against SQL injection attacks

- CLI: command line interface to initialize your database and launch the dashboard with one simple command

- Observable: Dashboard with real-time data, metrics, and query inspection

- Free & Open Source: MIT Licensed

## TODO

- [ ] Improve CLI: add --help/--version and generate schema.py + skypydb.db under _generated
- [ ] Create the dashboard using Reflex
- [ ] update the documentation

## What's next!

- give us ideas!

## Cli

- use the cli to initialize your database and launch the dashboard with one simple command

```bash
skypydb dev
```

- run this command in your terminal

## API

- use the api to interact with your database, before using it, make sure to create add a schema to create your tables

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
client = skypydb.Client(path="./skypydb/skypydb.db")

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
for user_success_logs in user_success_logs:
    print(user_success_logs)
```

- you can also delete specific data from your database using the delete method

```python
success_table.delete(
    component="AuthService",
    user_id="user123"
)
```

### Secure Implementation

- first create a encryption key and make it available in .env file don't show this key to anyone, you can use the cli to generate these keys

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
    path="./skypydb/skypydb.db",
    encryption_key=encryption_key,
    salt=salt_bytes,
    encrypted_fields=["user_id"]  # Optional: encrypt only sensitive fields
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
    message="User logged in successfully",
    user_id="user123" # only this field is encrypted if encrypted_fields is not None
)

# Data is automatically decrypted when retrieved
user_success_logs = success_table.search(
    index="by_user",
    user_id="user123"
)
for user_success_logs in user_success_logs:
    print(user_success_logs)
```

Learn more on our [Docs](https://ahen.mintlify.app/)

## License

[MIT](./LICENSE)
