import os
import skypydb
from skypydb.errors import TableAlreadyExistsError
from dotenv import load_dotenv

# Load environment variables from .env file
load_dotenv()

# Load encryption key from environment
encryption_key = os.getenv("ENCRYPTION_KEY") # create a encryption key and make it available in .env file before using it, don't show this key to anyone

# Create encrypted database
client = skypydb.Client(
    path="./data/secure.db",
    encryption_key=encryption_key,
    encrypted_fields=["password", "ssn", "credit_card"]  # Optional: encrypt only sensitive fields
)

# All operations work the same - encryption is transparent!
try:
    table = client.create_table("users")# Create the table.
except TableAlreadyExistsError:
    # Tables already exist, that's fine
    pass
    
table = client.get_table("users")

# Automatically encrypted
table.add(
    username=["alice"],
    email=["alice@example.com"],
    ssn=["123-45-6789"]  # only this field is if encrypted_fields is not None encrypted
)

# Data is automatically decrypted when retrieved
results = table.search(
    index="alice"# search the corresponding data by their index
)
for result in results:
    print(result)
