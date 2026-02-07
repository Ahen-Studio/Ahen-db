"""
Skypydb API Server
"""

import os
import sys
from typing import (
    Dict,
    Any,
    Optional
)
from fastapi import (
    FastAPI,
    HTTPException,
    Header
)
from fastapi.middleware.cors import CORSMiddleware
import uvicorn
from skypydb.server.dashboard_server import DashboardAPI

app = FastAPI(
    title="SkypyDB Dashboard API",
    description="REST API for monitoring SkypyDB databases",
    version="1.0.0"
)

app.add_middleware(
    CORSMiddleware,
    allow_origins=["http://localhost:3000"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)
dashboard_api = DashboardAPI()

def update_db_paths(
    main_path: Optional[str],
    vector_path: Optional[str]
):
    """
    Update database paths from request headers.
    """

    if main_path:
        os.environ['SKYPYDB_PATH'] = main_path
    if vector_path:
        os.environ['SKYPYDB_VECTOR_PATH'] = vector_path

@app.get("/api/health")
async def health_check(
    x_skypydb_path: Optional[str] = Header(None),
    x_skypydb_vector_path: Optional[str] = Header(None)
):
    """
    Check health status of all database components.
    """

    try:
        update_db_paths(x_skypydb_path, x_skypydb_vector_path)
        return dashboard_api.health.check()
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@app.get("/api/summary")
async def get_summary(
    x_skypydb_path: Optional[str] = Header(None),
    x_skypydb_vector_path: Optional[str] = Header(None)
):
    """
    Get quick summary of entire database system.
    """

    try:
        update_db_paths(x_skypydb_path, x_skypydb_vector_path)
        return dashboard_api.get_summary()
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@app.get("/api/statistics")
async def get_statistics(
    x_skypydb_path: Optional[str] = Header(None),
    x_skypydb_vector_path: Optional[str] = Header(None)
):
    """
    Get comprehensive statistics for all databases.
    """

    try:
        update_db_paths(x_skypydb_path, x_skypydb_vector_path)
        return dashboard_api.statistics.get_all()
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@app.get("/api/tables")
async def list_tables(
    x_skypydb_path: Optional[str] = Header(None)
):
    """
    Get all tables with metadata and row counts.
    """

    try:
        if x_skypydb_path:
            os.environ['SKYPYDB_PATH'] = x_skypydb_path
        return dashboard_api.tables.list_all()
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@app.get("/api/tables/{table_name}/schema")
async def get_table_schema(
    table_name: str,
    x_skypydb_path: Optional[str] = Header(None)
):
    """
    Get schema information for a table.
    """

    try:
        if x_skypydb_path:
            os.environ['SKYPYDB_PATH'] = x_skypydb_path
        return dashboard_api.tables.get_schema(table_name)
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@app.get("/api/tables/{table_name}/data")
async def get_table_data(
    table_name: str,
    limit: int = 100,
    offset: int = 0,
    x_skypydb_path: Optional[str] = Header(None)
):
    """
    Get paginated data from a table.
    """

    try:
        if x_skypydb_path:
            os.environ['SKYPYDB_PATH'] = x_skypydb_path
        return dashboard_api.tables.get_data(table_name, limit=limit, offset=offset)
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@app.get("/api/tables/{table_name}/search")
async def search_table(
    table_name: str,
    query: Optional[str] = None,
    limit: int = 100,
    x_skypydb_path: Optional[str] = Header(None)
):
    """
    Search table data with filters.
    """

    try:
        if x_skypydb_path:
            os.environ['SKYPYDB_PATH'] = x_skypydb_path
        return dashboard_api.tables.search(table_name, query=query, limit=limit)
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@app.get("/api/collections")
async def list_collections(
    x_skypydb_vector_path: Optional[str] = Header(None)
):
    """
    Get all vector collections with document counts.
    """

    try:
        if x_skypydb_vector_path:
            os.environ['SKYPYDB_VECTOR_PATH'] = x_skypydb_vector_path
        return dashboard_api.vector.list_all()
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@app.get("/api/collections/{collection_name}")
async def get_collection_details(
    collection_name: str,
    x_skypydb_vector_path: Optional[str] = Header(None)
):
    """
    Get detailed information about a vector collection.
    """

    try:
        if x_skypydb_vector_path:
            os.environ['SKYPYDB_VECTOR_PATH'] = x_skypydb_vector_path
        return dashboard_api.vector.get_details(collection_name)
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@app.post("/api/collections/{collection_name}/documents")
async def get_collection_documents(
    collection_name: str,
    body: Dict[str, Any],
    x_skypydb_vector_path: Optional[str] = Header(None)
):
    """
    Get documents from a vector collection.
    """

    try:
        if x_skypydb_vector_path:
            os.environ['SKYPYDB_VECTOR_PATH'] = x_skypydb_vector_path

        limit = body.get('limit', 100)
        offset = body.get('offset', 0)
        document_ids = body.get('document_ids')
        metadata_filter = body.get('metadata_filter')
        return dashboard_api.vector.get_documents(
            collection_name,
            document_ids=document_ids,
            metadata_filter=metadata_filter,
            limit=limit,
            offset=offset
        )
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@app.post("/api/collections/{collection_name}/search")
async def search_vectors(
    collection_name: str,
    body: Dict[str, Any],
    x_skypydb_vector_path: Optional[str] = Header(None)
):
    """
    Search for similar documents using vector similarity.
    """

    try:
        if x_skypydb_vector_path:
            os.environ['SKYPYDB_VECTOR_PATH'] = x_skypydb_vector_path

        query_text = body.get('query_text', '')
        n_results = body.get('n_results', 10)
        metadata_filter = body.get('metadata_filter')
        return dashboard_api.vector.search(
            collection_name,
            query_text,
            n_results=n_results,
            metadata_filter=metadata_filter
        )
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

if __name__ == "__main__":
    print("Starting SkypyDB API Server.")
    print("API will be available at: http://localhost:8000/api")
    print("\nEndpoints:")
    print("  - GET  /api/health")
    print("  - GET  /api/summary")
    print("  - GET  /api/statistics")
    print("  - GET  /api/tables")
    print("  - GET  /api/tables/{name}/schema")
    print("  - GET  /api/tables/{name}/data")
    print("  - GET  /api/collections")
    print("  - POST /api/collections/{name}/search")
    print("\nPress Ctrl+C to stop")
    
    uvicorn.run(
        app,
        host="0.0.0.0",
        port=8000
    )
