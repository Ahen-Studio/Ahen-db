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
