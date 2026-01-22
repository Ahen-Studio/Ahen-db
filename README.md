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

- CLI: command line interface for managing the database

- Observable: Dashboard with real-time data, metrics, and query inspection

- Free & Open Source: MIT Licensed

## TODO

- [x] code the database backend
- [x] improve user data security
- [ ] code a custom cli
- [ ] Create the dashboard using Reflex
- [ ] write the documentation

## What's next!

- give us ideas!

## Secure Implementation

- first create a encryption key and make it available in .env file don't show this key to anyone

```python
from skypydb.security import EncryptionManager

# Generate a secure encryption key
encryption_key = EncryptionManager.generate_key()
print(encryption_key) # don't show this key to anyone
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

# Create encrypted database
client = skypydb.Client(
    path="./data/secure.db",
    encryption_key=encryption_key,
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
client = skypydb.Client(path="./data/skypy.db", auto_start_dashboard=False)

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

# Start the Dashboard (blocking mode keeps it running)
client.start_dashboard(block=True)
```

- use the api without a custom config

```python
import skypydb
from skypydb.errors import TableAlreadyExistsError

# setup skypydb client.
client = skypydb.Client(path="./data/skypy.db", auto_start_dashboard=False)

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

# Start the Dashboard (blocking mode keeps it running)
client.start_dashboard(block=True)
```

## Dashboard

Start the dashboard to view your data in real time. It will remain active after your operations:

```python
import skypydb

# Dashboard will auto-start in non-blocking mode by default
client = skypydb.Client(path="./data/skypy.db")

# Or explicitly disable auto-start and start it later with blocking mode
client = skypydb.Client(path="./data/skypy.db", auto_start_dashboard=False)

# ... your operations here ...

# Start the Dashboard (blocking mode keeps it running)
client.start_dashboard(block=True)  # Dashboard accessible at http://127.0.0.1:3000
```

**Dashboard options:**

```python
# Change the dashboard port
client = skypydb.Client(
    path="./data/skypy.db",
    dashboard_port=8080,# Change the port of the dashboard
    auto_start_dashboard=False
)

# ... your operations here ...

# Start the Dashboard in blocking mode (keeps the program running)
client.start_dashboard(block=True)

# Or use non-blocking mode to continue with other operations
client.start_dashboard(block=False)
```

```python
# Disable auto-start
client = skypydb.Client(
    path="./data/skypy.db",
    auto_start_dashboard=False
)

# Start manually later
client.start_dashboard(block=False)

# Kill the dashboard later
client.stop_dashboard()
```

Learn more on our [Docs](https://ahen.mintlify.app/)

## License

[MIT](./LICENSE)
