//! Embedding provider implementations and factory helpers.

use crate::errors::{Result, SkypydbError};
use fastembed::{EmbeddingModel, InitOptions, TextEmbedding};
use reqwest::blocking::Client;
use reqwest::header::{HeaderMap, HeaderValue, AUTHORIZATION, CONTENT_TYPE};
use serde::Deserialize;
use serde_json::{json, Map, Value};
use std::str::FromStr;
use std::sync::{Arc, Mutex};
use std::time::Duration;

pub trait EmbeddingFunction: Send + Sync {
    fn embed(&self, texts: &[String]) -> Result<Vec<Vec<f32>>>;
}

#[derive(Clone)]
pub struct OllamaEmbedding {
    model: String,
    base_url: String,
    client: Client,
}

impl OllamaEmbedding {
    pub fn new(model: String, base_url: String) -> Result<Self> {
        let client = Client::builder().timeout(Duration::from_secs(60)).build()?;

        Ok(Self {
            model,
            base_url: base_url.trim_end_matches('/').to_string(),
            client,
        })
    }
}

impl EmbeddingFunction for OllamaEmbedding {
    fn embed(&self, texts: &[String]) -> Result<Vec<Vec<f32>>> {
        if texts.is_empty() {
            return Ok(Vec::new());
        }

        #[derive(Deserialize)]
        struct OllamaEmbeddingResponse {
            embedding: Option<Vec<f32>>,
        }
        let mut embeddings = Vec::with_capacity(texts.len());

        for text in texts {
            let response = self
                .client
                .post(format!("{}/api/embeddings", self.base_url))
                .json(&json!({
                    "model": self.model,
                    "prompt": text,
                }))
                .send()
                .map_err(|error| {
                    SkypydbError::embedding(format!(
                        "Cannot connect to Ollama at {}. Make sure Ollama is running. Error: {error}",
                        self.base_url
                    ))
                })?;
            if !response.status().is_success() {
                return Err(SkypydbError::embedding(format!(
                    "Ollama embedding request failed with status {}",
                    response.status()
                )));
            }
            let parsed: OllamaEmbeddingResponse = response.json().map_err(|error| {
                SkypydbError::embedding(format!("Invalid response from Ollama: {error}"))
            })?;
            let Some(embedding) = parsed.embedding else {
                return Err(SkypydbError::embedding(
                    "No embedding returned from Ollama. Make sure the selected model is an embedding model.",
                ));
            };

            embeddings.push(embedding);
        }

        Ok(embeddings)
    }
}

#[derive(Clone)]
pub struct OpenAIEmbedding {
    api_key: String,
    model: String,
    base_url: String,
    organization: Option<String>,
    project: Option<String>,
    client: Client,
}

impl OpenAIEmbedding {
    #[allow(clippy::too_many_arguments)]
    pub fn new(
        api_key: Option<String>,
        model: String,
        base_url: Option<String>,
        organization: Option<String>,
        project: Option<String>,
        timeout_seconds: Option<f64>,
    ) -> Result<Self> {
        let api_key = api_key
            .or_else(|| std::env::var("OPENAI_API_KEY").ok())
            .ok_or_else(|| {
                SkypydbError::embedding(
                    "OpenAI API key is required. Provide `api_key` or set OPENAI_API_KEY.",
                )
            })?;
        let timeout = Duration::from_secs_f64(timeout_seconds.unwrap_or(60.0));
        let client = Client::builder().timeout(timeout).build()?;

        Ok(Self {
            api_key,
            model,
            base_url: base_url
                .unwrap_or_else(|| "https://api.openai.com/v1".to_string())
                .trim_end_matches('/')
                .to_string(),
            organization,
            project,
            client,
        })
    }
}

impl EmbeddingFunction for OpenAIEmbedding {
    fn embed(&self, texts: &[String]) -> Result<Vec<Vec<f32>>> {
        if texts.is_empty() {
            return Ok(Vec::new());
        }

        #[derive(Deserialize)]
        struct OpenAIEmbeddingItem {
            embedding: Vec<f32>,
        }

        #[derive(Deserialize)]
        struct OpenAIEmbeddingResponse {
            data: Vec<OpenAIEmbeddingItem>,
        }
        let mut headers = HeaderMap::new();
        headers.insert(CONTENT_TYPE, HeaderValue::from_static("application/json"));
        headers.insert(
            AUTHORIZATION,
            HeaderValue::from_str(&format!("Bearer {}", self.api_key))
                .map_err(|error| SkypydbError::embedding(error.to_string()))?,
        );
        if let Some(organization) = &self.organization {
            headers.insert(
                "OpenAI-Organization",
                HeaderValue::from_str(organization)
                    .map_err(|error| SkypydbError::embedding(error.to_string()))?,
            );
        }
        if let Some(project) = &self.project {
            headers.insert(
                "OpenAI-Project",
                HeaderValue::from_str(project)
                    .map_err(|error| SkypydbError::embedding(error.to_string()))?,
            );
        }
        let response = self
            .client
            .post(format!("{}/embeddings", self.base_url))
            .headers(headers)
            .json(&json!({
                "model": self.model,
                "input": texts,
            }))
            .send()?;
        if !response.status().is_success() {
            let status = response.status();
            let body = response.text().unwrap_or_default();
            return Err(SkypydbError::embedding(format!(
                "OpenAI embedding request failed with status {status}: {body}"
            )));
        }
        let parsed: OpenAIEmbeddingResponse = response.json()?;

        Ok(parsed.data.into_iter().map(|item| item.embedding).collect())
    }
}

#[derive(Clone)]
pub struct SentenceTransformerEmbedding {
    model: String,
    normalize_embeddings: bool,
    embedder: Arc<Mutex<TextEmbedding>>,
}

fn normalize_embedding(values: &mut [f32]) {
    let norm = values.iter().map(|value| value * value).sum::<f32>().sqrt();
    if norm <= f32::EPSILON {
        return;
    }
    for value in values {
        *value /= norm;
    }
}

fn normalize_model_name(model_name: &str) -> String {
    model_name.trim().to_ascii_lowercase().replace('_', "-")
}

fn model_leaf_alias(model_code: &str) -> String {
    model_code
        .rsplit('/')
        .next()
        .unwrap_or(model_code)
        .trim_end_matches("-onnx")
        .to_string()
}

fn resolve_fastembed_model(model_name: &str) -> Result<EmbeddingModel> {
    let normalized_requested = normalize_model_name(model_name);
    if normalized_requested.is_empty() {
        return Err(SkypydbError::validation(
            "Model name cannot be empty for sentence-transformers provider.",
        ));
    }
    if let Ok(model) = EmbeddingModel::from_str(model_name) {
        return Ok(model);
    }
    let requested_leaf = model_leaf_alias(&normalized_requested);

    for model_info in TextEmbedding::list_supported_models() {
        let normalized_code = normalize_model_name(&model_info.model_code);
        if normalized_code == normalized_requested {
            return Ok(model_info.model.clone());
        }
        if model_leaf_alias(&normalized_code) == requested_leaf {
            return Ok(model_info.model.clone());
        }
    }

    Err(SkypydbError::validation(format!(
        "Unsupported sentence-transformers model '{model_name}'. Use a model name from fastembed (for example: 'all-MiniLM-L6-v2', 'BAAI/bge-base-en-v1.5', or 'Qdrant/all-MiniLM-L6-v2-onnx').",
    )))
}

impl SentenceTransformerEmbedding {
    pub fn new(model: String, normalize_embeddings: bool) -> Result<Self> {
        let embedding_model = resolve_fastembed_model(&model)?;
        let embedder =
            TextEmbedding::try_new(InitOptions::new(embedding_model)).map_err(|error| {
                SkypydbError::embedding(format!(
                    "Failed to initialize fastembed model '{model}': {error}"
                ))
            })?;

        Ok(Self {
            model,
            normalize_embeddings,
            embedder: Arc::new(Mutex::new(embedder)),
        })
    }
}

impl EmbeddingFunction for SentenceTransformerEmbedding {
    fn embed(&self, texts: &[String]) -> Result<Vec<Vec<f32>>> {
        if texts.is_empty() {
            return Ok(Vec::new());
        }
        let mut embedder = self.embedder.lock().map_err(|error| {
            SkypydbError::embedding(format!(
                "Sentence-transformers model lock poisoned: {error}"
            ))
        })?;
        let mut embeddings = embedder.embed(texts, None).map_err(|error| {
            SkypydbError::embedding(format!(
                "Sentence Transformers embedding provider failed for model '{}': {error}",
                self.model
            ))
        })?;
        if self.normalize_embeddings {
            for embedding in &mut embeddings {
                normalize_embedding(embedding);
            }
        }

        Ok(embeddings)
    }
}

pub fn get_embedding_function(
    provider: &str,
    mut config: Map<String, Value>,
) -> Result<Arc<dyn EmbeddingFunction>> {
    let provider = provider.to_lowercase().trim().replace('_', "-");
    if provider == "ollama" {
        let model = take_string(&mut config, "model", "mxbai-embed-large")?;
        let base_url = take_string(&mut config, "base_url", "http://localhost:11434")?;
        validate_remaining_config("ollama", &config)?;
        return Ok(Arc::new(OllamaEmbedding::new(model, base_url)?));
    }
    if provider == "openai" {
        let api_key = take_optional_string(&mut config, "api_key")?;
        let model = take_string(&mut config, "model", "text-embedding-3-small")?;
        let base_url = take_optional_string(&mut config, "base_url")?;
        let organization = take_optional_string(&mut config, "organization")?;
        let project = take_optional_string(&mut config, "project")?;
        let timeout = take_optional_f64(&mut config, "timeout")?;
        validate_remaining_config("openai", &config)?;
        return Ok(Arc::new(OpenAIEmbedding::new(
            api_key,
            model,
            base_url,
            organization,
            project,
            timeout,
        )?));
    }
    if provider == "sentence-transformers" || provider == "sentence-transformer" {
        let model = take_string(&mut config, "model", "all-MiniLM-L6-v2")?;
        let normalize_embeddings = take_bool(&mut config, "normalize_embeddings", false)?;
        validate_remaining_config("sentence-transformers", &config)?;
        return Ok(Arc::new(SentenceTransformerEmbedding::new(
            model,
            normalize_embeddings,
        )?));
    }

    Err(SkypydbError::embedding(format!(
        "Unsupported embedding provider '{provider}'. Supported providers: ollama, openai, sentence-transformers."
    )))
}

fn validate_remaining_config(provider: &str, config: &Map<String, Value>) -> Result<()> {
    if config.is_empty() {
        return Ok(());
    }

    let unsupported_keys = config.keys().cloned().collect::<Vec<_>>().join(", ");
    Err(SkypydbError::validation(format!(
        "Unsupported embedding config keys for provider '{provider}': {unsupported_keys}"
    )))
}

fn take_string(config: &mut Map<String, Value>, key: &str, default: &str) -> Result<String> {
    match config.remove(key) {
        Some(Value::String(value)) => Ok(value),
        Some(value) => Ok(value.to_string()),

        None => Ok(default.to_string()),
    }
}

fn take_optional_string(config: &mut Map<String, Value>, key: &str) -> Result<Option<String>> {
    match config.remove(key) {
        Some(Value::String(value)) => Ok(Some(value)),
        Some(Value::Null) => Ok(None),
        Some(value) => Ok(Some(value.to_string())),

        None => Ok(None),
    }
}

fn take_optional_f64(config: &mut Map<String, Value>, key: &str) -> Result<Option<f64>> {
    match config.remove(key) {
        Some(Value::Number(value)) => value.as_f64().map(Some).ok_or_else(|| {
            SkypydbError::validation(format!("Invalid numeric value for key '{key}'"))
        }),
        Some(Value::String(value)) => value
            .parse::<f64>()
            .map(Some)
            .map_err(|_| SkypydbError::validation(format!("Invalid float value for key '{key}'"))),
        Some(Value::Null) | None => Ok(None),
        Some(_) => Err(SkypydbError::validation(format!(
            "Invalid value type for key '{key}', expected float"
        ))),
    }
}

fn take_bool(config: &mut Map<String, Value>, key: &str, default: bool) -> Result<bool> {
    match config.remove(key) {
        Some(Value::Bool(value)) => Ok(value),
        Some(Value::String(value)) => {
            let normalized = value.to_lowercase();

            Ok(matches!(normalized.as_str(), "true" | "1" | "yes"))
        }
        Some(Value::Number(value)) => Ok(value.as_i64().unwrap_or_default() != 0),
        Some(Value::Null) | None => Ok(default),
        Some(_) => Err(SkypydbError::validation(format!(
            "Invalid value type for key '{key}', expected bool"
        ))),
    }
}
