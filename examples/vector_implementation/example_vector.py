import skypydb

# Create a client
client = skypydb.VectorClient()

# Create a collection
collection = client.get_or_create_collection("my-documents")

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
if not results:
    print("No results found.")
else:
    for i, doc_id in enumerate(results["ids"][0]):
        print(f"{doc_id}, {results['documents'][0][i]}, {results['distances'][0][i]}")