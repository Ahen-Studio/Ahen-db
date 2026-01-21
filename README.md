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

- Table: create, update, delete data from a table

- Observable: Dashboard with real-time data, metrics, and query inspection

- Free & Open Source: MIT Licensed

## TODO

- [ ] code the database backend
- [ ] Create the dashboard using Reflex
- [ ] write the documentation
- [ ] improve user data security
- [ ] code a custom cli

## What's next!

- give us ideas!

## API

- use the api with a custom config

```python
import skypydb

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
except Exception:
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

# Keep the program running so the dashboard stays active
client.wait()
```

- use the api without a custom config

```python
import skypydb

# setup skypydb client.
client = skypydb.Client(path="./data/skypy.db")

# Create table. get_table, delete_table are also available.
try:
    table = client.create_table("all-my-documents")
except Exception:
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

# Keep the program running so the dashboard stays active
client.wait()
```

## Dashboard

The dashboard starts automatically when you create a client. To keep it running after your operations:

```python
import skypydb

client = skypydb.Client(path="./data/skypy.db")

# ... your operations here ...

# Keep the dashboard active
client.wait()  # Dashboard accessible at http://127.0.0.1:3000
```

**Dashboard options:**

```python
# Change the dashboard port
client = skypydb.Client(
    path="./data/skypy.db",
    dashboard_port=8080
)

# Disable auto-start
client = skypydb.Client(
    path="./data/skypy.db",
    auto_start_dashboard=False
)

# Start manually later
client.start_dashboard()
```

Learn more on our [Docs](https://ahen.mintlify.app/)

## License

[MIT](./LICENSE)
