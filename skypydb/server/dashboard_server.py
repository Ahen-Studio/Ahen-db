"""
Dashboard API for monitoring Skypydb databases.
"""

import os
from pathlib import Path
import time
from typing import (
    Dict,
    List,
    Optional,
    Any
)
from dataclasses import dataclass
from skypydb.database.reactive_db import ReactiveDatabase
from skypydb.database.vector_db import VectorDatabase
from skypydb.database.database_linker import DatabaseLinker

@dataclass
class TableInfo:
    """
    Information about a database table.
    """

    name: str
    row_count: int
    columns: List[str]
    config: Optional[Dict] = None

@dataclass
class VectorCollectionInfo:
    """
    Information about a vector collection.
    """

    name: str
    document_count: int
    metadata: Dict[str, Any]

@dataclass
class PaginatedResult:
    """
    Paginated query result.
    """

    data: List[Dict]
    total: int
    limit: int
    offset: int
    has_more: bool

class DatabaseConnection:
    """
    Manages database connections.
    """

    @staticmethod
    def _resolve_db_path(
        env_key: str,
        default_relative: str,
        db_type: str
    ) -> str:
        """
        Resolve a database path from env, normalizing to an absolute path.
        """

        base = Path.cwd()
        raw = os.environ.get(env_key)
        if raw:
            path = Path(raw)
            return str(path if path.is_absolute() else (base / path).resolve())

        discovered = DatabaseConnection.discover_links()
        for item in discovered:
            if item.get("type") != db_type:
                continue
            candidate = Path(item.get("path", ""))
            if candidate.exists():
                return str(candidate.resolve())

        default_path = (base / default_relative).resolve()
        if default_path.exists():
            return str(default_path)

        generated_dir = (base / "db" / "_generated")
        if generated_dir.exists():
            candidates = sorted(p for p in generated_dir.glob("*.db") if p.is_file())
            if candidates:
                return str(candidates[0].resolve())
        return str(default_path)

    @staticmethod
    def discover_links() -> List[Dict[str, str]]:
    ) -> List[Dict[str, str]]:
        """
        Discover database link metadata from current working directory.
        """

        self.database_linker = DatabaseLinker()
        return self.database_linker.discover_database_links(Path.cwd())

    @staticmethod
    def _require_existing(path: str, label: str) -> None:
        """
        Ensure the database file exists before connecting.
        """

        if not Path(path).exists():
            raise FileNotFoundError(
                f"{label} database file not found at: {path}. "
                "Set the correct path with the request headers or env vars."
            )

    @staticmethod
    def get_main() -> ReactiveDatabase:
        """
        Get main database instance from environment.
        """

        path = DatabaseConnection._resolve_db_path(
            "SKYPYDB_PATH",
            "db/_generated/skypydb.db",
            "reactive"
        )
        DatabaseConnection._require_existing(path, "Main")
        return ReactiveDatabase(path)

    @staticmethod
    def get_vector() -> VectorDatabase:
        """
        Get vector database instance from environment.
        """

        path = DatabaseConnection._resolve_db_path(
            "SKYPYDB_VECTOR_PATH",
            "db/_generated/vector.db",
            "vector"
        )
        DatabaseConnection._require_existing(path, "Vector")
        return VectorDatabase(path)

class LinksAPI:
    """
    API for database type/path discovery metadata.
    """

    def list_all(self) -> List[Dict[str, str]]:
        """
        List discovered DB links from the project root.
        """

        return DatabaseConnection.discover_links()

class HealthAPI:
    """
    API for checking system health status.
    """

    def check(self) -> Dict[str, Any]:
        """
        Check health status of all database components.
        
        Returns:
            Dictionary with timestamp, overall status, and database statuses
        """

        status = {
            "timestamp": time.time_ns(),
            "status": "healthy",
            "databases": {}
        }

        self._check_main(status)
        self._check_vector(status)
        return status

    def _check_main(
        self,
        status: Dict[str, Any]
    ) -> None:
        """
        Check main database health.
        """

        try:
            db = DatabaseConnection.get_main()
            table_count = len(db.get_all_tables_names())
            db.close()

            status["databases"]["main"] = {
                "status": "connected",
                "tables": table_count
            }
        except Exception as error:
            status["databases"]["main"] = {
                "status": "error",
                "error": str(error)
            }
            status["status"] = "degraded"

    def _check_vector(
        self,
        status: Dict[str, Any]
    ) -> None:
        """
        Check vector database health.
        """

        vdb = None
        try:
            vdb = DatabaseConnection.get_vector()
            collection_count = len(vdb.list_collections())

            status["databases"]["vector"] = {
                "status": "connected",
                "collections": collection_count
            }
        except Exception as error:
            status["databases"]["vector"] = {
                "status": "error",
                "error": str(error)
            }
            status["status"] = "degraded"
        finally:
            if vdb is not None:
                try:
                    vdb.close()
                except Exception:
                    # suppress close errors in health check to avoid masking original issues
                    pass

class TableAPI:
    """
    API for table operations.
    """

    def list_all(self) -> List[Dict[str, Any]]:
        """
        Get all tables with metadata and row counts.
        """

        db = DatabaseConnection.get_main()
        
        try:
            table_names = db.get_all_tables_names()
            return [self._get_info(db, name) for name in table_names]
        finally:
            db.close()

    def _get_info(
        self,
        db: ReactiveDatabase,
        table_name: str
    ) -> Dict[str, Any]:
        """
        Get information about a specific table.
        """

        try:
            return {
                "name": table_name,
                "row_count": len(db.get_all_data(table_name)),
                "columns": db.get_table_columns(table_name),
                "config": db.get_table_config(table_name)
            }
        except Exception:
            return {
                "name": table_name,
                "row_count": 0,
                "columns": [],
                "config": None
            }

    def get_schema(
        self,
        table_name: str
    ) -> Dict[str, Any]:
        """
        Get schema information for a table.
        """

        db = DatabaseConnection.get_main()

        try:
            return {
                "name": table_name,
                "columns": db.get_table_columns(table_name),
                "config": db.get_table_config(table_name)
            }
        finally:
            db.close()

    def get_data(
        self,
        table_name: str,
        limit: int = 100,
        offset: int = 0
    ) -> Dict[str, Any]:
        """
        Get paginated data from a table.
        """

        db = DatabaseConnection.get_main()

        try:
            all_data = db.get_all_data(table_name)
            return self._paginate(all_data, limit, offset)
        finally:
            db.close()

    def search(
        self,
        table_name: str,
        query: Optional[str] = None,
        limit: int = 100,
        **filters
    ) -> Dict[str, Any]:
        """
        Search table data with filters.
        """

        db = DatabaseConnection.get_main()

        try:
            results = db.search(table_name, index=query, **filters)
            if limit and len(results) > limit:
                results = results[:limit]
            return {
                "data": results,
                "total": len(results),
                "limit": limit
            }
        finally:
            db.close()

    def _paginate(
        self,
        data: List[Dict],
        limit: int,
        offset: int
    ) -> Dict[str, Any]:
        """
        Apply pagination to data.
        """

        total = len(data)
        start = offset
        end = offset + limit if limit else total
        return {
            "data": data[start:end],
            "total": total,
            "limit": limit,
            "offset": offset,
            "has_more": end < total
        }

class VectorAPI:
    """
    API for vector collection operations.
    """

    def list_all(self) -> List[Dict[str, Any]]:
        """
        Get all vector collections with document counts.
        """

        vdb = DatabaseConnection.get_vector()

        try:
            collections = vdb.list_collections()
            return [self._get_info(vdb, coll) for coll in collections]
        except Exception:
            return []
        finally:
            vdb.close()

    def _get_info(
        self,
        vdb: VectorDatabase,
        collection: Dict
    ) -> Dict[str, Any]:
        """
        Get information about a vector collection.
        """

        name = collection['name']
        
        try:
            return {
                "name": name,
                "document_count": vdb.count(name),
                "metadata": collection.get('metadata', {})
            }
        except Exception:
            return {
                "name": name,
                "document_count": 0,
                "metadata": collection.get('metadata', {})
            }

    def get_details(
        self,
        collection_name: str
    ) -> Dict[str, Any]:
        """
        Get detailed information about a vector collection.
        """

        vdb = DatabaseConnection.get_vector()

        try:
            collection = vdb.get_collection(collection_name)
            if collection is None:
                return {
                    "name": collection_name,
                    "exists": False,
                    "error": "Collection not found"
                }
            return {
                "name": collection_name,
                "exists": True,
                "document_count": vdb.count(collection_name),
                "metadata": collection.get('metadata', {})
            }
        except Exception as error:
            return {
                "name": collection_name,
                "exists": False,
                "error": str(error)
            }
        finally:
            vdb.close()

    def get_documents(
        self,
        collection_name: str,
        document_ids: Optional[List[str]] = None,
        metadata_filter: Optional[Dict] = None,
        limit: int = 100,
        offset: int = 0
    ) -> Dict[str, Any]:
        """
        Get documents from a vector collection.
        """

        vdb = DatabaseConnection.get_vector()

        try:
            results = vdb.get(
                collection_name,
                ids=document_ids,
                where=metadata_filter,
                include=["documents", "metadatas"]
            )
            return self._paginate(results, limit, offset)
        except Exception as error:
            return self._empty_result(error)
        finally:
            vdb.close()

    def search(
        self,
        collection_name: str,
        query_text: str,
        n_results: int = 10,
        metadata_filter: Optional[Dict] = None
    ) -> Dict[str, Any]:
        """
        Search for similar documents using vector similarity.
        """

        vdb = DatabaseConnection.get_vector()

        try:
            results = vdb.query(
                collection_name,
                query_texts=[query_text],
                n_results=n_results,
                where=metadata_filter,
                include=["documents", "metadatas", "distances"]
            )
            return self._format_results(results, query_text, n_results)
        except Exception as error:
            return {
                "results": [],
                "query": query_text,
                "error": str(error)
            }
        finally:
            vdb.close()

    def _paginate(
        self,
        results: Dict,
        limit: int,
        offset: int
    ) -> Dict[str, Any]:
        """
        Apply pagination to vector results.
        """

        total = len(results.get("ids", []))
        start = offset
        end = offset + limit if limit else total
        return {
            "ids": results["ids"][start:end],
            "documents": results.get("documents", [])[start:end],
            "metadatas": results.get("metadatas", [])[start:end],
            "total": total,
            "limit": limit,
            "offset": offset,
            "has_more": end < total
        }

    def _format_results(
        self,
        results: Dict,
        query_text: str,
        n_results: int
    ) -> Dict[str, Any]:
        """
        Format vector search results.
        """

        ids = results.get("ids", [[]])[0]
        documents = results.get("documents", [[]])[0]
        metadatas = results.get("metadatas", [[]])[0] if results.get("metadatas") else [None] * len(ids)
        distances = results.get("distances", [[]])[0] if results.get("distances") else [None] * len(ids)

        formatted = [
            {
                "id": ids[i],
                "document": documents[i],
                "metadata": metadatas[i],
                "similarity_score": distances[i]
            }
            for i in range(len(ids))
        ]
        return {
            "results": formatted,
            "query": query_text,
            "n_results": n_results
        }

    def _empty_result(
        self,
        error: Exception
    ) -> Dict[str, Any]:
        """
        Return empty result with error.
        """

        return {
            "ids": [],
            "documents": [],
            "metadatas": [],
            "total": 0,
            "error": str(error)
        }

class StatisticsAPI:
    """
    API for database statistics.
    """

    def get_all(self) -> Dict[str, Any]:
        """
        Get comprehensive statistics for all databases.
        """

        stats = {
            "timestamp": time.time_ns(),
            "tables": {"count": 0, "total_rows": 0},
            "collections": {"count": 0, "total_documents": 0}
        }

        self._collect_tables(stats)
        self._collect_collections(stats)
        return stats

    def _collect_tables(
        self,
        stats: Dict[str, Any]
    ) -> None:
        """
        Collect table statistics.
        """

        try:
            db = DatabaseConnection.get_main()
            table_names = db.get_all_tables_names()

            stats["tables"]["count"] = len(table_names)
            stats["tables"]["total_rows"] = sum(
                len(db.get_all_data(table))
                for table in table_names
            )

            db.close()
        except Exception as error:
            stats["tables"]["error"] = str(error)

    def _collect_collections(
        self,
        stats: Dict[str, Any]
    ) -> None:
        """
        Collect collection statistics.
        """

        vdb = None
        try:
            vdb = DatabaseConnection.get_vector()
            collections = vdb.list_collections()

            stats["collections"]["count"] = len(collections)
            stats["collections"]["total_documents"] = sum(
                vdb.count(coll['name'])
                for coll in collections
            )
        except Exception as error:
            stats["collections"]["error"] = str(error)
        finally:
            if vdb is not None:
                try:
                    vdb.close()
                except Exception:
                    # suppress close errors to avoid masking original exceptions
                    pass

class DashboardAPI:
    """
    Main Dashboard API class providing access to all monitoring operations.

    Organizes functionality into logical groups:
        health: System health monitoring
        tables: Table operations
        vector: Vector collection operations
        statistics: Database-wide statistics

    Example:
        api = DashboardAPI()

        # Check health
        health = api.health.check()

        # List tables
        tables = api.tables.list_all()

        # Get table data
        data = api.tables.get_data("users", limit=50)

        # List collections
        collections = api.vector.list_all()

        # Search vectors
        results = api.vector.search("docs", "machine learning")

        # Get statistics
        stats = api.statistics.get_all()
    """

    def __init__(self):
        """
        Initialize Dashboard API with all sub-APIs.
        """

        self.health = HealthAPI()
        self.tables = TableAPI()
        self.vector = VectorAPI()
        self.statistics = StatisticsAPI()
        self.links = LinksAPI()

    def get_summary(self) -> Dict[str, Any]:
        """
        Get quick summary of entire database system.

        Returns:
            Dictionary with health status and key metrics
        """

        health = self.health.check()
        stats = self.statistics.get_all()
        return {
            "status": health["status"],
            "timestamp": health["timestamp"],
            "summary": {
                "tables": stats["tables"],
                "collections": stats["collections"]
            },
            "health_details": health["databases"],
            "database_links": self.links.list_all()
        }
