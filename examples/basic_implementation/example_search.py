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

# Ensure there is at least one row to demonstrate search
success_table.add(
    component="AuthService",
    action="login",
    message="User logged in successfully",
    user_id="user123"
)

# Search results by filter
user_success_logs = success_table.search(
    user_id="user123"
)

if not user_success_logs:
    print("No results found.")
else:
    for user_success_log in user_success_logs:
        print(user_success_log)
