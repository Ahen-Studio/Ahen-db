from mem0 import Memory

# Local mem0 config
config = {
    "vector_store": {
        "provider": "skypydb",
        "config": {
            "collection_name": "memory",
            "path": "db"
        }
    },
    "llm": {
        "provider": "ollama",
        "config": {
            "model": "llama3.1:latest",
            "temperature": 0.3,
            "max_tokens": 1024,
            "ollama_base_url": "http://localhost:11434",
        },
    },
    "embedder": {
        "provider": "ollama",
        "config": {
            "model": "mxbai-embed-large"
        }
    }
}

m = Memory.from_config(config)

# Add memories
m.add("I love Python programming", user_id="user1")
m.add("My favorite color is blue", user_id="user1")

# Search memories
results = m.search("What programming language do I like?", user_id="user1")

print(results)
