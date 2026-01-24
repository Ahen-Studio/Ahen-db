import skypydb

# Create a client
client = skypydb.Client(path="./skypydb/skypydb.db")

# Create tables from the schema
# This reads the schema from skypydb/schema.py and creates all tables
tables = client.create_table()

# Access your tables
success_table = tables["success"]
warning_table = tables["warning"]
error_table = tables["error"]

# before searching you need to add data to the table
# Search results. search data assuming they are already data in the table.
user_success_logs = success_table.search(
    index="by_user",
    user_id="user123"
)
for user_success_log in user_success_logs:
    print(user_success_log)
