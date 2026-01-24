import os
import skypydb
from dotenv import load_dotenv

# Load environment variables from .env file
load_dotenv()

# Load encryption key from environment
encryption_key = os.getenv("ENCRYPTION_KEY") # create a encryption key and make it available in .env file before using it, don't show this key to anyone
salt_key = os.getenv("SALT_KEY") # create a salt key and make it available in .env file before using it, don't show this salt to anyone

# transform salt key to bytes
if salt_key is None:
    raise ValueError("SALT_KEY missing")
salt_bytes = salt_key.encode("utf-8")

# Create encrypted database
client = skypydb.Client(
    path="./skypydb/skypydb.db",
    encryption_key=encryption_key,
    salt=salt_bytes,
    encrypted_fields=["message"]  # Optional: encrypt only sensitive fields
)

# All operations work the same - encryption is transparent!
tables = client.create_table()

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
    index="by_user",
    user_id="user123"
)
for user_success_log in user_success_logs:
    print(user_success_log)
