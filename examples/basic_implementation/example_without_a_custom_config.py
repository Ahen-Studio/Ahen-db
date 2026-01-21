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
