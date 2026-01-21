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

- create a declarative schema system to customize the tables

- Add the ability to delete specific data in a table

- Add the ability to update specific data in a table

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
table = client.create_table_from_config(config)# Create all the tables present in the config.
#table = client.get_table_from_config(config, table_name="all-my-documents")
#table = client.delete_table_from_config(config, table_name="all-my-documents")

# Add data to a table.

# Retrieve the table before adding any data.
table = client.get_table_from_config(config, table_name="all-my-documents")

table.add(
    title=["document"],
    user_id=["user123"],
    content=["this is a document"],
    id=["auto"]# ids are automatically created by the backend.
)

# Search results. You can also search the data by the id of the document.
results = table.search(
    index="user123",
    title=["document"]# Search the corresponding data by their title.
    #id=["***"]
)
```

- use the api without a custom config

```python
import skypydb

# setup skypydb client.
client = skypydb.Client(path="./data/skypy.db")

# Create table. get_table, delete_table are also available.
table = client.create_table("all-my-documents")
#table = client.get_table("all-my-documents")
#table = client.delete_table("all-my-documents")

# Add data to the table.
table.add(
    title=["document"],
    user_id=["user123"],
    content=["this is a document"],
    id=["auto"]# ids are automatically created by the backend
)

# Search results. You can also search the data by the id of the document
results = table.search(
    index="user123",
    title=["document"]# search the corresponding data by their title
    #id=["***"]
)
```

Learn more on our [Docs](https://ahen.mintlify.app/)

## License

[MIT](./LICENSE)
