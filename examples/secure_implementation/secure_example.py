import os
import skypydb
from dotenv import load_dotenv

# Load environment variables from .env file
load_dotenv(".env.local")

# Load encryption key from environment
encryption_key = os.getenv("ENCRYPTION_KEY") # create a encryption key and make it available in .env file before using it, don't show this key to anyone
salt_key = os.getenv("SALT_KEY") # create a salt key and make it available in .env file before using it, don't show this salt to anyone

# transform salt key to bytes
if salt_key is None:
    raise ValueError("SALT_KEY missing")
salt_bytes = salt_key.encode("utf-8")

# Create encrypted database
client = skypydb.ReactiveClient(
    encryption_key=encryption_key,
    salt=salt_bytes,
    encrypted_fields=["message"]  # Optional: encrypt only sensitive fields
)

# All operations work the same - encryption is transparent!
tables = client.get_or_create_table()
# if the tables already exists the programe get them instead

# Access your tables
success_table = tables["success"]
warning_table = tables["warning"]
error_table = tables["error"]

# Automatically encrypted
success_table.add(
    component="AuthService",
    action="login",
    message="User logged in successfully", # only this field is encrypted if encrypted_fields is not None
    user_id="user123"
)

# Data is automatically decrypted when retrieved
user_success_logs = success_table.search(
    user_id="user123"
)

if not user_success_logs:
    print("No results found.")
else:
    for user_success_log in user_success_logs:
        print(user_success_log)
