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

# delete data on the table assuming they are already in the table.
table.delete(
    title=["document"],
    user_id=["user123"],
    content=["this is a document"],
)
