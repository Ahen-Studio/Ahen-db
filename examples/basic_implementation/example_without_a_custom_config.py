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

# Keep the program running so the dashboard stays active
client.wait()
