//! HTTP server for the dashboard API.

use super::dashboard_api::DashboardApi;
use crate::errors::{Result, SkypydbError};
use axum::extract::{Path, Query, State};
use axum::http::{HeaderMap, StatusCode};
use axum::routing::{get, post};
use axum::{Json, Router};
use reqwest::Method;
use serde::Deserialize;
use serde_json::{json, Value};
use std::path::Path as StdPath;
use std::process::Stdio;
use std::time::Duration;
use tokio::process::{Child, Command};
use tokio::sync::oneshot;
use tower_http::cors::{Any, CorsLayer};

#[derive(Clone)]
struct AppState {
    api: DashboardApi,
}

type ApiResponse = std::result::Result<Json<Value>, (StatusCode, Json<Value>)>;

#[derive(Debug, Deserialize)]
struct TableDataQuery {
    limit: Option<usize>,
    offset: Option<usize>,
}

#[derive(Debug, Deserialize)]
struct TableSearchQuery {
    query: Option<String>,
    limit: Option<usize>,
}

#[derive(Debug, Deserialize)]
struct CollectionDocumentsBody {
    limit: Option<usize>,
    offset: Option<usize>,
    document_ids: Option<Vec<String>>,
    metadata_filter: Option<Value>,
}

#[derive(Debug, Deserialize)]
struct CollectionSearchBody {
    query_text: Option<String>,
    n_results: Option<usize>,
    metadata_filter: Option<Value>,
}

pub fn build_router(api: DashboardApi) -> Router {
    let state = AppState { api };

    Router::new()
        .route("/api/health", get(health_check))
        .route("/api/summary", get(get_summary))
        .route("/api/statistics", get(get_statistics))
        .route("/api/databaselinks", get(get_database_links))
        .route("/api/tables", get(list_tables))
        .route("/api/tables/:table_name/schema", get(get_table_schema))
        .route("/api/tables/:table_name/data", get(get_table_data))
        .route("/api/tables/:table_name/search", get(search_table))
        .route("/api/collections", get(list_collections))
        .route(
            "/api/collections/:collection_name",
            get(get_collection_details),
        )
        .route(
            "/api/collections/:collection_name/documents",
            post(get_collection_documents),
        )
        .route(
            "/api/collections/:collection_name/search",
            post(search_vectors),
        )
        .layer(
            CorsLayer::new()
                .allow_origin(
                    "http://localhost:3000"
                        .parse::<axum::http::HeaderValue>()
                        .expect("valid origin"),
                )
                .allow_methods(Any)
                .allow_headers(Any),
        )
        .with_state(state)
}

pub async fn run_dashboard_server(host: &str, port: u16) -> Result<()> {
    let api = DashboardApi::new();
    let app = build_router(api);

    let address = format!("{host}:{port}");
    let listener = tokio::net::TcpListener::bind(&address)
        .await
        .map_err(|error| SkypydbError::database(error.to_string()))?;

    let (shutdown_tx, shutdown_rx) = oneshot::channel::<()>();
    let mut shutdown_tx = Some(shutdown_tx);

    let mut server_task = tokio::spawn(async move {
        axum::serve(listener, app)
            .with_graceful_shutdown(async move {
                let _ = shutdown_rx.await;
            })
            .await
    });

    let base_url = format!("http://127.0.0.1:{port}");
    wait_for_backend_ready(&base_url).await?;
    check_dashboard_endpoints(&base_url).await?;

    let mut frontend: Child = start_dashboard_frontend(3000).await?;

    println!("Backend API running on http://{host}:{port}");
    println!("Frontend running with `npm run dev` in db/_generated/dashboard");
    println!("Press Ctrl+C to stop both backend and frontend.");

    tokio::select! {
        backend_result = &mut server_task => {
            if frontend.id().is_some() {
                let _ = frontend.kill().await;
                let _ = frontend.wait().await;
            }

            match backend_result {
                Ok(Ok(())) => Ok(()),
                Ok(Err(error)) => Err(SkypydbError::database(error.to_string())),
                Err(error) => Err(SkypydbError::database(format!("Backend task failed: {error}"))),
            }
        }
        frontend_result = frontend.wait() => {
            if let Some(tx) = shutdown_tx.take() {
                let _ = tx.send(());
            }

            let frontend_status = frontend_result
                .map_err(|error| SkypydbError::database(error.to_string()))?;

            let backend_status = server_task
                .await
                .map_err(|error| SkypydbError::database(format!("Backend task failed: {error}")))?;

            if let Err(error) = backend_status {
                return Err(SkypydbError::database(error.to_string()));
            }

            if frontend_status.success() {
                Ok(())
            } else {
                Err(SkypydbError::database(format!(
                    "Frontend exited with status: {frontend_status}"
                )))
            }
        }
        ctrl_c_result = tokio::signal::ctrl_c() => {
            ctrl_c_result.map_err(|error| SkypydbError::database(error.to_string()))?;
            if let Some(tx) = shutdown_tx.take() {
                let _ = tx.send(());
            }

            if frontend.id().is_some() {
                let _ = frontend.kill().await;
                let _ = frontend.wait().await;
            }

            let backend_status = server_task
                .await
                .map_err(|error| SkypydbError::database(format!("Backend task failed: {error}")))?;

            backend_status
                .map_err(|error| SkypydbError::database(error.to_string()))?;
            Ok(())
        }
    }
}

async fn wait_for_backend_ready(base_url: &str) -> Result<()> {
    let client = reqwest::Client::builder()
        .timeout(Duration::from_secs(3))
        .build()
        .map_err(|error| SkypydbError::database(error.to_string()))?;

    let deadline = tokio::time::Instant::now() + Duration::from_secs(15);
    loop {
        let response = client.get(format!("{base_url}/api/health")).send().await;
        if let Ok(response) = response {
            if response.status().is_success() {
                return Ok(());
            }
        }

        if tokio::time::Instant::now() >= deadline {
            return Err(SkypydbError::database(
                "Backend API did not become ready within 15 seconds.",
            ));
        }

        tokio::time::sleep(Duration::from_millis(250)).await;
    }
}

async fn check_dashboard_endpoints(base_url: &str) -> Result<()> {
    let client = reqwest::Client::builder()
        .timeout(Duration::from_secs(8))
        .build()
        .map_err(|error| SkypydbError::database(error.to_string()))?;

    request_endpoint_json(
        &client,
        Method::GET,
        &format!("{base_url}/api/health"),
        None,
    )
    .await?;
    request_endpoint_json(
        &client,
        Method::GET,
        &format!("{base_url}/api/summary"),
        None,
    )
    .await?;
    request_endpoint_json(
        &client,
        Method::GET,
        &format!("{base_url}/api/statistics"),
        None,
    )
    .await?;
    request_endpoint_json(
        &client,
        Method::GET,
        &format!("{base_url}/api/databaselinks"),
        None,
    )
    .await?;

    let tables_payload = request_endpoint_json(
        &client,
        Method::GET,
        &format!("{base_url}/api/tables"),
        None,
    )
    .await?;
    let collections_payload = request_endpoint_json(
        &client,
        Method::GET,
        &format!("{base_url}/api/collections"),
        None,
    )
    .await?;

    if let Some(table_name) = first_item_name(&tables_payload) {
        request_endpoint_json(
            &client,
            Method::GET,
            &format!("{base_url}/api/tables/{table_name}/schema"),
            None,
        )
        .await?;
        request_endpoint_json(
            &client,
            Method::GET,
            &format!("{base_url}/api/tables/{table_name}/data"),
            None,
        )
        .await?;
        request_endpoint_json(
            &client,
            Method::GET,
            &format!("{base_url}/api/tables/{table_name}/search?query=test&limit=1"),
            None,
        )
        .await?;
    }
    if let Some(collection_name) = first_item_name(&collections_payload) {
        request_endpoint_json(
            &client,
            Method::GET,
            &format!("{base_url}/api/collections/{collection_name}"),
            None,
        )
        .await?;
        request_endpoint_json(
            &client,
            Method::POST,
            &format!("{base_url}/api/collections/{collection_name}/documents"),
            Some(json!({
                "limit": 1,
                "offset": 0,
                "document_ids": [],
                "metadata_filter": null
            })),
        )
        .await?;
        request_endpoint_json(
            &client,
            Method::POST,
            &format!("{base_url}/api/collections/{collection_name}/search"),
            Some(json!({
                "query_text": "test",
                "n_results": 1,
                "metadata_filter": null
            })),
        )
        .await?;
    }

    println!("All backend endpoints responded successfully.");

    Ok(())
}

fn first_item_name(payload: &Value) -> Option<&str> {
    payload
        .as_array()
        .and_then(|items| items.first())
        .and_then(|item| item.get("name"))
        .and_then(Value::as_str)
}

async fn request_endpoint_json(
    client: &reqwest::Client,
    method: Method,
    url: &str,
    body: Option<Value>,
) -> Result<Value> {
    let mut request = client.request(method, url);
    if let Some(body) = body {
        request = request.json(&body);
    }

    let response = request.send().await.map_err(|error| {
        SkypydbError::database(format!("Endpoint check failed for {url}: {error}"))
    })?;

    if response.status() == reqwest::StatusCode::NOT_FOUND
        || response.status() == reqwest::StatusCode::METHOD_NOT_ALLOWED
    {
        return Err(SkypydbError::database(format!(
            "Endpoint check failed for {url}: unexpected status {}",
            response.status()
        )));
    }

    response.json::<Value>().await.map_err(|error| {
        SkypydbError::database(format!(
            "Endpoint check returned non-JSON for {url}: {error}"
        ))
    })
}

async fn start_dashboard_frontend(frontend_port: u16) -> Result<Child> {
    let cwd = std::env::current_dir().map_err(|error| SkypydbError::database(error.to_string()))?;
    let dashboard_dir = cwd.join("db").join("_generated").join("dashboard");
    if !dashboard_dir.exists() {
        return Err(SkypydbError::database(format!(
            "Dashboard folder not found at {}. Run `skypydbrust init` first.",
            dashboard_dir.display()
        )));
    }

    let npm_executable = find_npm_executable().await.ok_or_else(|| {
        SkypydbError::database(
            "npm was not found in PATH. Install Node.js/npm to launch the dashboard frontend.",
        )
    })?;

    ensure_frontend_dependencies(&dashboard_dir, &npm_executable).await?;

    let mut command = Command::new(&npm_executable);
    command
        .arg("run")
        .arg("dev")
        .arg("--")
        .arg("--port")
        .arg(frontend_port.to_string())
        .current_dir(&dashboard_dir)
        .stdin(Stdio::null())
        .stdout(Stdio::inherit())
        .stderr(Stdio::inherit());

    command
        .spawn()
        .map_err(|error| SkypydbError::database(format!("Failed to start frontend: {error}")))
}

async fn find_npm_executable() -> Option<String> {
    for candidate in ["npm", "npm.cmd"] {
        let status: std::io::Result<std::process::ExitStatus> = Command::new(candidate)
            .arg("--version")
            .stdout(Stdio::null())
            .stderr(Stdio::null())
            .status()
            .await;

        if matches!(status, Ok(status) if status.success()) {
            return Some(candidate.to_string());
        }
    }
    None
}

async fn ensure_frontend_dependencies(dashboard_dir: &StdPath, npm_executable: &str) -> Result<()> {
    let next_unix = dashboard_dir.join("node_modules").join(".bin").join("next");
    let next_windows = dashboard_dir
        .join("node_modules")
        .join(".bin")
        .join("next.cmd");

    if next_unix.exists() || next_windows.exists() {
        return Ok(());
    }

    println!(
        "Installing dashboard dependencies in {} ...",
        dashboard_dir.display()
    );

    let status: std::io::Result<std::process::ExitStatus> = Command::new(npm_executable)
        .arg("install")
        .current_dir(dashboard_dir)
        .stdin(Stdio::null())
        .stdout(Stdio::inherit())
        .stderr(Stdio::inherit())
        .status()
        .await;

    let status = status
        .map_err(|error| SkypydbError::database(format!("Failed to run npm install: {error}")))?;

    if !status.success() {
        return Err(SkypydbError::database(format!(
            "Dashboard dependency installation failed with status: {status}"
        )));
    }

    Ok(())
}

async fn health_check(State(state): State<AppState>, headers: HeaderMap) -> ApiResponse {
    let main_path = header_value(&headers, "x-skypydb-path");
    let vector_path = header_value(&headers, "x-skypydb-vector-path");

    Ok(Json(
        state
            .api
            .health
            .check(main_path.as_deref(), vector_path.as_deref()),
    ))
}

async fn get_summary(State(state): State<AppState>, headers: HeaderMap) -> ApiResponse {
    let main_path = header_value(&headers, "x-skypydb-path");
    let vector_path = header_value(&headers, "x-skypydb-vector-path");

    Ok(Json(
        state
            .api
            .get_summary(main_path.as_deref(), vector_path.as_deref()),
    ))
}

async fn get_statistics(State(state): State<AppState>, headers: HeaderMap) -> ApiResponse {
    let main_path = header_value(&headers, "x-skypydb-path");
    let vector_path = header_value(&headers, "x-skypydb-vector-path");

    Ok(Json(
        state
            .api
            .statistics
            .get_all(main_path.as_deref(), vector_path.as_deref()),
    ))
}

async fn get_database_links(State(state): State<AppState>) -> ApiResponse {
    Ok(Json(state.api.links.list_all()))
}

async fn list_tables(State(state): State<AppState>, headers: HeaderMap) -> ApiResponse {
    let main_path = header_value(&headers, "x-skypydb-path");
    state
        .api
        .tables
        .list_all(main_path.as_deref())
        .map(Json)
        .map_err(internal_error)
}

async fn get_table_schema(
    State(state): State<AppState>,
    Path(table_name): Path<String>,
    headers: HeaderMap,
) -> ApiResponse {
    let main_path = header_value(&headers, "x-skypydb-path");
    state
        .api
        .tables
        .get_schema(&table_name, main_path.as_deref())
        .map(Json)
        .map_err(internal_error)
}

async fn get_table_data(
    State(state): State<AppState>,
    Path(table_name): Path<String>,
    Query(params): Query<TableDataQuery>,
    headers: HeaderMap,
) -> ApiResponse {
    let main_path = header_value(&headers, "x-skypydb-path");
    let limit = params.limit.unwrap_or(100);
    let offset = params.offset.unwrap_or(0);

    state
        .api
        .tables
        .get_data(&table_name, limit, offset, main_path.as_deref())
        .map(Json)
        .map_err(internal_error)
}

async fn search_table(
    State(state): State<AppState>,
    Path(table_name): Path<String>,
    Query(params): Query<TableSearchQuery>,
    headers: HeaderMap,
) -> ApiResponse {
    let main_path = header_value(&headers, "x-skypydb-path");
    let limit = params.limit.unwrap_or(100);

    state
        .api
        .tables
        .search(&table_name, params.query, limit, None, main_path.as_deref())
        .map(Json)
        .map_err(internal_error)
}

async fn list_collections(State(state): State<AppState>, headers: HeaderMap) -> ApiResponse {
    let vector_path = header_value(&headers, "x-skypydb-vector-path");
    state
        .api
        .vector
        .list_all(vector_path.as_deref())
        .map(Json)
        .map_err(internal_error)
}

async fn get_collection_details(
    State(state): State<AppState>,
    Path(collection_name): Path<String>,
    headers: HeaderMap,
) -> ApiResponse {
    let vector_path = header_value(&headers, "x-skypydb-vector-path");
    Ok(Json(
        state
            .api
            .vector
            .get_details(&collection_name, vector_path.as_deref()),
    ))
}

async fn get_collection_documents(
    State(state): State<AppState>,
    Path(collection_name): Path<String>,
    headers: HeaderMap,
    Json(body): Json<CollectionDocumentsBody>,
) -> ApiResponse {
    let vector_path = header_value(&headers, "x-skypydb-vector-path");
    let limit = body.limit.unwrap_or(100);
    let offset = body.offset.unwrap_or(0);

    Ok(Json(state.api.vector.get_documents(
        &collection_name,
        body.document_ids,
        body.metadata_filter,
        limit,
        offset,
        vector_path.as_deref(),
    )))
}

async fn search_vectors(
    State(state): State<AppState>,
    Path(collection_name): Path<String>,
    headers: HeaderMap,
    Json(body): Json<CollectionSearchBody>,
) -> ApiResponse {
    let vector_path = header_value(&headers, "x-skypydb-vector-path");
    let query_text = body.query_text.unwrap_or_default();
    let n_results = body.n_results.unwrap_or(10);

    Ok(Json(state.api.vector.search(
        &collection_name,
        &query_text,
        n_results,
        body.metadata_filter,
        vector_path.as_deref(),
    )))
}

fn header_value(headers: &HeaderMap, name: &str) -> Option<String> {
    headers
        .get(name)
        .and_then(|value| value.to_str().ok())
        .map(|value| value.to_string())
}

fn internal_error(error: SkypydbError) -> (StatusCode, Json<Value>) {
    (
        StatusCode::INTERNAL_SERVER_ERROR,
        Json(json!({
            "detail": error.to_string(),
        })),
    )
}
