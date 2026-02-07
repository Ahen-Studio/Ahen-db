import skypydb

# Create a client
client = skypydb.ReactiveClient()

# Create tables from the schema
# This reads the schema from db/schema.py and creates all tables
tables = client.get_or_create_table()
# if the tables already exists the programe get them instead

# Access your tables
success_table = tables["success"]
warning_table = tables["warning"]
error_table = tables["error"]

# delete data on the table assuming they are already data in the table.
success_table.delete(
    component="AuthService",
    user_id="user123"
)
