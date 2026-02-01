"""
Dashboard API for monitoring Skypydb databases.
"""

import os
import time
from typing import Dict, List, Optional, Any
from dataclasses import dataclass
from ..db.database import Database
from ..db.vector_database import VectorDatabase


# data classes
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


# database connection
class DatabaseConnection:
    """
    Manages database connections.
    """

    @staticmethod
    def get_main() -> Database:
        """
        Get main database instance from environment.
        """

        path = os.environ.get('SKYPYDB_PATH', './db/_generated/skypydb.db')
        return Database(path)

    @staticmethod
    def get_vector() -> VectorDatabase:
        """
        Get vector database instance from environment.
        """

        path = os.environ.get('SKYPYDB_VECTOR_PATH', './db/_generated/vector.db')
        return VectorDatabase(path)


# health monitoring
class HealthAPI:
    """
    API for checking system health status.
    """

    # check health status of all database components
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


    # check main database health
    def _check_main(self, status: Dict[str, Any]) -> None:
        """
        Check main database health.
        """

        try:
            db = DatabaseConnection.get_main()
            table_count = len(db.get_all_tables())
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


    # check vector database health
    def _check_vector(self, status: Dict[str, Any]) -> None:
        """
        Check vector database health.
        """

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


# table operations
class TableAPI:
    """
    API for table operations.
    """

    # list all tables in the database with metadata and row counts
    def list_all(self) -> List[Dict[str, Any]]:
        """
        Get all tables with metadata and row counts.
        """

        db = DatabaseConnection.get_main()
        
        try:
            table_names = db.get_all_tables()
            return [self._get_info(db, name) for name in table_names]
        finally:
            db.close()


    # get information about a specific table
    def _get_info(self, db: Database, table_name: str) -> Dict[str, Any]:
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


    # get the schema information for a table
    def get_schema(self, table_name: str) -> Dict[str, Any]:
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


    # get a specific number of data from a table
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


    # search for data in a table
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


    # apply pagination to data
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


# vector collection operations
class VectorAPI:
    """
    API for vector collection operations.
    """

    # list all vector collections with document counts
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


    # get information about a vector collection
    def _get_info(self, vdb: VectorDatabase, collection: Dict) -> Dict[str, Any]:
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


    # get details about a vector collection
    def get_details(self, collection_name: str) -> Dict[str, Any]:
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


    # get documents from a vector collection
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


    # search for similar documents using vector similarity
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


    # apply pagination to vector results
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


    # format vector search results
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


    # format empty search results
    def _empty_result(self, error: Exception) -> Dict[str, Any]:
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


# statistics
class StatisticsAPI:
    """
    API for database statistics.
    """


    # get all statistics
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


    # collect tables statistics
    def _collect_tables(self, stats: Dict[str, Any]) -> None:
        """
        Collect table statistics.
        """

        try:
            db = DatabaseConnection.get_main()
            table_names = db.get_all_tables()

            stats["tables"]["count"] = len(table_names)
            stats["tables"]["total_rows"] = sum(
                len(db.get_all_data(table))
                for table in table_names
            )

            db.close()
        except Exception as error:
            stats["tables"]["error"] = str(error)


    # collect collections statistics
    def _collect_collections(self, stats: Dict[str, Any]) -> None:
        """
        Collect collection statistics.
        """

        try:
        try:
            vdb = DatabaseConnection.get_vector()
            collections = vdb.list_collections()

            stats["collections"]["count"] = len(collections)
            stats["collections"]["total_documents"] = sum(
                vdb.count(coll['name'])
                for coll in collections
            )
            vdb.close()
        except Exception as error:
            stats["collections"]["error"] = str(error)


# main api class
class DashboardAPI:
    """
    Main Dashboard API class providing access to all monitoring operations.

    Organizes functionality into logical groups:
    - health: System health monitoring
    - tables: Table operations
    - vector: Vector collection operations
    - statistics: Database-wide statistics

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

    # initialize the dashboard API
    def __init__(self):
        """
        Initialize Dashboard API with all sub-APIs.
        """

        self.health = HealthAPI()
        self.tables = TableAPI()
        self.vector = VectorAPI()
        self.statistics = StatisticsAPI()


    # get quick summary of entire database system
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
            "health_details": health["databases"]
        }
