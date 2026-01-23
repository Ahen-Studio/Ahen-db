<h1><div align="center">
 <img alt="Skypy" width="auto" height="auto" src="https://github.com/amichyrpi/skypy-db/blob/main/docs/logo/dark.svg#gh-light-mode-only">
 <img alt="Skypy" width="auto" height="auto" src="https://github.com/amichyrpi/skypy-db/blob/main/docs/logo/dark.svg#gh-dark-mode-only">
</div></h1>

<p align="center">
    <b>Skypy - open-source reactive database</b>. <br />
    The better way to build Python logging system!
</p>

<p align="center">
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

- CLI: command line interface to initialize your database

- Observable: Dashboard with real-time data, metrics, and query inspection

- Free & Open Source: MIT Licensed

## TODO

- [x] code the database backend
- [x] improve user data security
- [x] code a custom cli
- [ ] Create the dashboard using Reflex
- [ ] write the documentation

## What's next!

- give us ideas!

## Cli

- use the cli to initialize your database and launch the dashboard with one simple command

```bash
skypydb dev
```

- run this command in your terminal

## Secure Implementation

- first create a encryption key and make it available in .env file don't show this key to anyone

```python
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
from skypydb.errors import TableAlreadyExistsError
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
    path="./data/secure.db",
    encryption_key=encryption_key,
    salt=salt_bytes,
    encrypted_fields=["password", "ssn", "credit_card"]  # Optional: encrypt only sensitive fields
)

# All operations work the same - encryption is transparent!
try:
    table = client.create_table("users")# Create the table.
except TableAlreadyExistsError:
    # Tables already exist, that's fine
    pass
    
table = client.get_table("users")

# Automatically encrypted
table.add(
    username=["alice"],
    email=["alice@example.com"],
    ssn=["123-45-6789"]  # only this field is if encrypted_fields is not None encrypted
)

# Data is automatically decrypted when retrieved
results = table.search(
    index="alice"# search the corresponding data by their index
)
for result in results:
    print(result)
```

## API

- use the api with a custom config

```python
import skypydb
from skypydb.errors import TableAlreadyExistsError

# setup skypydb client.
client = skypydb.Client(path="./data/skypy.db")

# config to make custom table.
config = {
    "all-my-documents": {
        "title": "str",
        "user_id": str,
        "content": str,
        "id": "auto"
    },
    "all-my-documents1": {
        "title": "str",
        "user_id": str,
        "content": str,
        "id": "auto"
    },
    "all-my-documents2": {
        "title": "str",
        "user_id": str,
        "content": str,
        "id": "auto"
    },
}

# Create tables. get_table_from_config(config, table_name="all-my-documents"), delete_table_from_config(config, table_name="all-my-documents") are also available.
try:
    table = client.create_table_from_config(config)# Create all the tables present in the config.
except TableAlreadyExistsError:
    # Tables already exist, that's fine
    pass

# Retrieve the table before adding any data.
table = client.get_table_from_config(config, table_name="all-my-documents")

# Add data to a table.
table.add(
    title=["document"],
    user_id=["user123"],
    content=["this is a document"],
    id=["auto"]# ids are automatically created by the backend.
)

```

- use the api without a custom config

```python
import skypydb
from skypydb.errors import TableAlreadyExistsError

# setup skypydb client.
client = skypydb.Client(path="./data/skypy.db")

# Create table. get_table, delete_table are also available.
try:
    table = client.create_table("all-my-documents")
except TableAlreadyExistsError:
    # Tables already exist, that's fine
    pass

# Retrieve the table before adding any data.
table = client.get_table("all-my-documents")

# Add data to the table.
table.add(
    title=["document"],
    user_id=["user123"],
    content=["this is a document"],
    id=["auto"]# ids are automatically created by the backend
)

```

Learn more on our [Docs](https://ahen.mintlify.app/)

## License

[MIT](./LICENSE)
