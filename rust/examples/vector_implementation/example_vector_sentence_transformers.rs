use serde_json::json;
use skypydb::{json_map, Result, VectorClient};

fn main() -> Result<()> {
    // Create a new vector client with sentence transformers
    let client = VectorClient::new(
        "./db/_generated/vector_rust_sentence_transformers.db",
        "sentence-transformers",
        Some(json_map! {
            "model" => "all-MiniLM-L6-v2",
        }),
    )?;

    // Get or create collection
    let collection = client.get_or_create_collection("my-documents", None)?;

    // Add documents to the collection
    collection.add(
        vec!["doc1".to_string(), "doc2".to_string()],
        None,
        Some(vec![
            "This is document1".to_string(),
            "This is document2".to_string(),
        ]),
        Some(vec![
            json!({"source": "notion"}),
            json!({"source": "google-docs"}),
        ]),
    )?;

    // Query the collection
    let results = collection.query(
        None,
        Some(vec!["This is a query document".to_string()]),
        2,
        None,
        None,
        None,
    )?;

    // Print the results
    if results
        .ids
        .first()
        .map(|ids| ids.is_empty())
        .unwrap_or(true)
    {
        println!("No results found.");
    } else {
        for (index, id) in results.ids[0].iter().enumerate() {
            let document = results
                .documents
                .as_ref()
                .and_then(|docs| docs.first())
                .and_then(|docs| docs.get(index))
                .and_then(|doc| doc.as_ref())
                .cloned()
                .unwrap_or_default();
            let distance = results
                .distances
                .as_ref()
                .and_then(|distances| distances.first())
                .and_then(|distances| distances.get(index))
                .copied()
                .unwrap_or_default();

            println!("{id}, {document}, {distance}");
        }
    }

    Ok(())
}