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

# before searching you need to add data to the table
#table.add(
#    index=["user123"],
#    title=["document"],
#    content=["this is a document"],
#    id=["auto"]# ids are automatically created by the backend.
#)

# Search results. search data assuming they are already in the table. You can also search the data by the id of the document
results = table.search(
    index="user123",# search the corresponding data by their index
    title=["document"]# search the corresponding data by their title
    #id=["***"]
)
for result in results:
    print(result)
