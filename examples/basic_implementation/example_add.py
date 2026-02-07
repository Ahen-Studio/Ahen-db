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

# Insert data
# Insert success logs
success_table.add(
    component="AuthService",
    action="login",
    message="User logged in successfully",
    user_id="user123"
)

# Insert warning logs
warning_table.add(
    component="AuthService",
    action="login_attempt",
    message="Multiple failed login attempts",
    user_id="user456",
    details="5 failed attempts in 5 minutes"
)

# Insert error logs
error_table.add(
    component="DatabaseService",
    action="connection",
    message="Connection timeout",
    user_id="system",
    details="Timeout after 30 seconds"
)
