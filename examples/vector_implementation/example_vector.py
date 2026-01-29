from skypydb import Vector_Client

# Create a client
client = Vector_Client(path="./vector_db")

# Create a collection
collection = client.create_collection("my-documents")

# Add documents (automatically embedded using Ollama)
collection.add(
    documents=["This is document1", "This is document2"],
    metadatas=[{"source": "notion"}, {"source": "google-docs"}],
    ids=["doc1", "doc2"]
)

# Query for similar documents
results = collection.query(
    query_texts=["This is a query document"],
    n_results=2
)

# Access results
for i, doc_id in enumerate(results["ids"][0]):
    print(f"ID: {doc_id}")
    print(f"Document: {results['documents'][0][i]}")
    print(f"Distance: {results['distances'][0][i]}")
