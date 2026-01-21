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

# Keep the program running so the dashboard stays active
client.wait()
