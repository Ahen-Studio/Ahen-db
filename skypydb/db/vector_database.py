"""
Vector database backend using SQLite for storing and querying embeddings.
"""

import sqlite3
import json
import math
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, List, Optional, Callable
from ..security.validation import InputValidator


# calculate cosine similarity between two vectors
def cosine_similarity(vec1: List[float], vec2: List[float]) -> float:
    """
    Calculate cosine similarity between two vectors.

    Args:
        vec1: First vector
        vec2: Second vector

    Returns:
        Cosine similarity score between -1 and 1
    """

    if len(vec1) != len(vec2):
        raise ValueError(f"Vector dimensions don't match: {len(vec1)} vs {len(vec2)}")

    dot_product = sum(a * b for a, b in zip(vec1, vec2))
    norm1 = math.sqrt(sum(a * a for a in vec1))
    norm2 = math.sqrt(sum(b * b for b in vec2))

    if norm1 == 0 or norm2 == 0:
        return 0.0

    return dot_product / (norm1 * norm2)


# calculate euclidean distance between two vectors
def euclidean_distance(vec1: List[float], vec2: List[float]) -> float:
    """
    Calculate Euclidean distance between two vectors.

    Args:
        vec1: First vector
        vec2: Second vector

    Returns:
        Euclidean distance (lower is more similar)
    """

    if len(vec1) != len(vec2):
        raise ValueError(f"Vector dimensions don't match: {len(vec1)} vs {len(vec2)}")

    return math.sqrt(sum((a - b) ** 2 for a, b in zip(vec1, vec2)))


class VectorDatabase:
    """
    Manages SQLite database for vector storage and similarity search.

    Stores embeddings as JSON arrays and performs similarity search
    using cosine similarity or Euclidean distance.
    """

    def __init__(
        self,
        path: str,
        embedding_function: Optional[Callable[[List[str]], List[List[float]]]] = None,
    ):
        """
        Initialize vector database.

        Args:
            path: Path to SQLite database file
            embedding_function: Optional function to generate embeddings from text
        """

        self.path = path
        self.embedding_function = embedding_function

        # Create directory if it doesn't exist
        Path(path).parent.mkdir(parents=True, exist_ok=True)

        # Connect to SQLite database
        self.conn = sqlite3.connect(path, check_same_thread=False)
        self.conn.row_factory = sqlite3.Row

        # Create collections metadata table
        self._ensure_collections_table()


    # ensure the collections metadata table exists
    def _ensure_collections_table(self) -> None:
        """
        Ensure the collections metadata table exists.
        """

        cursor = self.conn.cursor()

        cursor.execute("""
            CREATE TABLE IF NOT EXISTS _vector_collections (
                name TEXT PRIMARY KEY,
                metadata TEXT,
                created_at TEXT NOT NULL
            )
        """)
        self.conn.commit()


    # check if a collection exists
    def collection_exists(self, name: str) -> bool:
        """
        Check if a collection exists.

        Args:
            name: Collection name

        Returns:
            True if collection exists
        """

        cursor = self.conn.cursor()

        cursor.execute(
            "SELECT name FROM sqlite_master WHERE type='table' AND name=?",
            (f"vec_{name}",)
        )
        return cursor.fetchone() is not None


    # create a collection
    def create_collection(
        self,
        name: str,
        metadata: Optional[Dict[str, Any]] = None,
    ) -> None:
        """
        Create a new vector collection.

        Args:
            name: Collection name
            metadata: Optional collection metadata

        Raises:
            ValueError: If collection already exists
        """

        name = InputValidator.validate_table_name(name)


        if self.collection_exists(name):
            raise ValueError(f"Collection '{name}' already exists")

        cursor = self.conn.cursor()

        # Create the collection table
        cursor.execute(f"""
            CREATE TABLE [vec_{name}] (
                id TEXT PRIMARY KEY,
                document TEXT,
                embedding TEXT NOT NULL,
                metadata TEXT,
                created_at TEXT NOT NULL
            )
        """)

        # Store collection metadata
        cursor.execute(
            "INSERT INTO _vector_collections (name, metadata, created_at) VALUES (?, ?, ?)",
            (name, json.dumps(metadata or {}), datetime.now().isoformat())
        )

        self.conn.commit()


    # get a collection
    def get_collection(self, name: str) -> Optional[Dict[str, Any]]:
        """
        Get collection metadata.

        Args:
            name: Collection name

        Returns:
            Collection metadata or None if not found
        """

        name = InputValidator.validate_table_name(name)

        if not self.collection_exists(name):
            return None

        cursor = self.conn.cursor()
        
        cursor.execute(
            "SELECT * FROM _vector_collections WHERE name = ?",
            (name,)
        )
        row = cursor.fetchone()

        if row:
            return {
                "name": row["name"],
                "metadata": json.loads(row["metadata"]) if row["metadata"] else {},
                "created_at": row["created_at"]
            }
        return None


    # get create or get a collection if it already exists
    def get_or_create_collection(
        self,
        name: str,
        metadata: Optional[Dict[str, Any]] = None,
    ) -> Dict[str, Any]:
        """
        Get an existing collection or create a new one.

        Args:
            name: Collection name
            metadata: Optional collection metadata (used only if creating)

        Returns:
            Collection metadata
        """

        name = InputValidator.validate_table_name(name)

        if not self.collection_exists(name):
            self.create_collection(name, metadata)

        result = self.get_collection(name)
        # At this point collection must exist since we just created it if needed
        assert result is not None
        return result


    # delete a collection and all its data
    def delete_collection(self, name: str) -> None:
        """
        Delete a collection and all its data.

        Args:
            name: Collection name

        Raises:
            ValueError: If collection doesn't exist
        """

        name = InputValidator.validate_table_name(name)


        if not self.collection_exists(name):
            raise ValueError(f"Collection '{name}' not found")

        cursor = self.conn.cursor()

        # Drop the collection table
        cursor.execute(f"DROP TABLE [vec_{name}]")

        # Remove from collections metadata
        cursor.execute(
            "DELETE FROM _vector_collections WHERE name = ?",
            (name,)
        )

        self.conn.commit()


    # list all collections present in the database
    def list_collections(self) -> List[Dict[str, Any]]:
        """
        List all collections.
        
        Returns:
            List of collection metadata dictionaries
        """

        cursor = self.conn.cursor()
        
        cursor.execute("SELECT * FROM _vector_collections")

        collections = []
        for row in cursor.fetchall():
            collections.append({
                "name": row["name"],
                "metadata": json.loads(row["metadata"]) if row["metadata"] else {},
                "created_at": row["created_at"]
            })

        return collections


    # add items to a collection
    def add(
        self,
        collection_name: str,
        ids: List[str],
        embeddings: Optional[List[List[float]]] = None,
        documents: Optional[List[str]] = None,
        metadatas: Optional[List[Dict[str, Any]]] = None,
    ) -> List[str]:
        """
        Add items to a collection.

        Args:
            collection_name: Name of the collection
            ids: List of unique IDs for each item
            embeddings: Optional list of embedding vectors
            documents: Optional list of documents (will be embedded if embedding_function is set)
            metadatas: Optional list of metadata dictionaries
            
        Returns:
            List of IDs of added items
            
        Raises:
            ValueError: If neither embeddings nor documents are provided
        """

        collection_name = InputValidator.validate_table_name(collection_name)

        if not self.collection_exists(collection_name):
            raise ValueError(f"Collection '{collection_name}' not found")

        if embeddings is None and documents is None:
            raise ValueError("Either embeddings or documents must be provided")

        if embeddings is None:
            if self.embedding_function is None:
                raise ValueError(
                    "Documents provided but no embedding function set. "
                    "Either provide embeddings directly or set an embedding_function."
                )
            if documents is None:
                raise ValueError("Either embeddings or documents must be provided")
            embeddings = self.embedding_function(documents)

        # Validate lengths match
        n_items = len(ids)
        if len(embeddings) != n_items:
            raise ValueError(
                f"Number of embeddings ({len(embeddings)}) doesn't match "
                f"number of IDs ({n_items})"
            )

        if documents is not None and len(documents) != n_items:
            raise ValueError(
                f"Number of documents ({len(documents)}) doesn't match "
                f"number of IDs ({n_items})"
            )

        if metadatas is not None and len(metadatas) != n_items:
            raise ValueError(
                f"Number of metadatas ({len(metadatas)}) doesn't match "
                f"number of IDs ({n_items})"
            )

        cursor = self.conn.cursor()
        
        now = datetime.now().isoformat()

        for i, item_id in enumerate(ids):
            embedding = embeddings[i]
            document = documents[i] if documents else None
            metadata = metadatas[i] if metadatas else None

            cursor.execute(
                f"""
                INSERT OR REPLACE INTO [vec_{collection_name}] 
                (id, document, embedding, metadata, created_at)
                VALUES (?, ?, ?, ?, ?)
                """,
                (
                    item_id,
                    document,
                    json.dumps(embedding),
                    json.dumps(metadata) if metadata else None,
                    now
                )
            )

        self.conn.commit()
        return ids

    
    # update items in a collection
    def update(
        self,
        collection_name: str,
        ids: List[str],
        embeddings: Optional[List[List[float]]] = None,
        documents: Optional[List[str]] = None,
        metadatas: Optional[List[Dict[str, Any]]] = None,
    ) -> None:
        """
        Update items in a collection.

        Args:
            collection_name: Name of the collection
            ids: List of IDs to update
            embeddings: Optional new embeddings
            documents: Optional new documents (will be embedded)
            metadatas: Optional new metadata
        """

        collection_name = InputValidator.validate_table_name(collection_name)

        if not self.collection_exists(collection_name):
            raise ValueError(f"Collection '{collection_name}' not found")

        if embeddings is None and documents is not None:
            if self.embedding_function is None:
                raise ValueError(
                    "Documents provided but no embedding function set."
                )
            embeddings = self.embedding_function(documents)

        cursor = self.conn.cursor()

        for i, item_id in enumerate(ids):
            updates = []
            params = []

            if embeddings is not None:
                updates.append("embedding = ?")
                params.append(json.dumps(embeddings[i]))

            if documents is not None:
                updates.append("document = ?")
                params.append(documents[i])

            if metadatas is not None:
                updates.append("metadata = ?")
                params.append(json.dumps(metadatas[i]) if metadatas[i] else None)

            if updates:
                params.append(item_id)
                cursor.execute(
                    f"UPDATE [vec_{collection_name}] SET {', '.join(updates)} WHERE id = ?",
                    params
                )

        self.conn.commit()


    # get items from a collection by ID or filter
    def get(
        self,
        collection_name: str,
        ids: Optional[List[str]] = None,
        where: Optional[Dict[str, Any]] = None,
        where_document: Optional[Dict[str, str]] = None,
        include: Optional[List[str]] = None,
    ) -> Dict[str, List[Any]]:
        """
        Get items from a collection by ID or filter.
        
        Args:
            collection_name: Name of the collection
            ids: Optional list of IDs to retrieve
            where: Optional metadata filter
            where_document: Optional document filter
            include: Optional list of fields to include (embeddings, documents, metadatas)
            
        Returns:
            Dictionary with lists of ids, embeddings, documents, and metadatas
        """

        collection_name = InputValidator.validate_table_name(collection_name)

        if not self.collection_exists(collection_name):
            raise ValueError(f"Collection '{collection_name}' not found")

        include = include or ["embeddings", "documents", "metadatas"]

        cursor = self.conn.cursor()

        if ids is not None:
            placeholders = ", ".join(["?" for _ in ids])
            cursor.execute(
                f"SELECT * FROM [vec_{collection_name}] WHERE id IN ({placeholders})",
                list(ids)
            )
        else:
            cursor.execute(f"SELECT * FROM [vec_{collection_name}]")

        results = {
            "ids": [],
            "embeddings": [] if "embeddings" in include else None,
            "documents": [] if "documents" in include else None,
            "metadatas": [] if "metadatas" in include else None,
        }

        for row in cursor.fetchall():
            item = {
                "id": row["id"],
                "document": row["document"],
                "embedding": json.loads(row["embedding"]),
                "metadata": json.loads(row["metadata"]) if row["metadata"] else None,
            }

            # Apply filters
            if not self._matches_filters(item, where, where_document):
                continue

            results["ids"].append(item["id"])

            if results["embeddings"] is not None:
                results["embeddings"].append(item["embedding"])

            if results["documents"] is not None:
                results["documents"].append(item["document"])

            if results["metadatas"] is not None:
                results["metadatas"].append(item["metadata"])

        return results


    # query the database collection for similar items
    def query(
        self,
        collection_name: str,
        query_embeddings: Optional[List[List[float]]] = None,
        query_texts: Optional[List[str]] = None,
        n_results: int = 10,
        where: Optional[Dict[str, Any]] = None,
        where_document: Optional[Dict[str, str]] = None,
        include: Optional[List[str]] = None,
    ) -> Dict[str, List[List[Any]]]:
        """
        Query a collection for similar items.
        
        Args:
            collection_name: Name of the collection
            query_embeddings: Optional list of query embeddings
            query_texts: Optional list of query texts (will be embedded)
            n_results: Number of results to return per query
            where: Optional metadata filter
            where_document: Optional document filter
            include: Optional list of fields to include
            
        Returns:
            Dictionary with nested lists of results for each query
        """
        
        collection_name = InputValidator.validate_table_name(collection_name)

        if not self.collection_exists(collection_name):
            raise ValueError(f"Collection '{collection_name}' not found")

        if query_embeddings is None and query_texts is None:
            raise ValueError("Either query_embeddings or query_texts must be provided")

        if query_embeddings is None:
            if self.embedding_function is None:
                raise ValueError(
                    "Query texts provided but no embedding function set."
                )
            if query_texts is None:
                raise ValueError("Either query_embeddings or query_texts must be provided")
            query_embeddings = self.embedding_function(query_texts)

        include = include or ["embeddings", "documents", "metadatas", "distances"]

        # Get all items from collection
        all_items = self._get_all_items(collection_name)

        results = {
            "ids": [],
            "embeddings": [] if "embeddings" in include else None,
            "documents": [] if "documents" in include else None,
            "metadatas": [] if "metadatas" in include else None,
            "distances": [] if "distances" in include else None,
        }

        for query_embedding in query_embeddings:
            # Calculate similarities and filter
            scored_items = []

            for item in all_items:
                # Apply filters
                if not self._matches_filters(item, where, where_document):
                    continue

                # Calculate cosine similarity
                similarity = cosine_similarity(query_embedding, item["embedding"])
                # Convert to distance (1 - similarity, so lower is better)
                distance = 1.0 - similarity

                scored_items.append((item, distance))

            # Sort by distance (ascending) and take top n
            scored_items.sort(key=lambda x: x[1])
            top_items = scored_items[:n_results]

            # Extract results for this query
            query_ids = []
            query_embeddings_result = []
            query_documents = []
            query_metadatas = []
            query_distances = []

            for item, distance in top_items:
                query_ids.append(item["id"])
                query_embeddings_result.append(item["embedding"])
                query_documents.append(item["document"])
                query_metadatas.append(item["metadata"])
                query_distances.append(distance)

            results["ids"].append(query_ids)

            if results["embeddings"] is not None:
                results["embeddings"].append(query_embeddings_result)

            if results["documents"] is not None:
                results["documents"].append(query_documents)

            if results["metadatas"] is not None:
                results["metadatas"].append(query_metadatas)

            if results["distances"] is not None:
                results["distances"].append(query_distances)

        return results


    # delete items from a collection
    def delete(
        self,
        collection_name: str,
        ids: Optional[List[str]] = None,
        where: Optional[Dict[str, Any]] = None,
        where_document: Optional[Dict[str, str]] = None,
    ) -> int:
        """
        Delete items from a collection.

        Args:
            collection_name: Name of the collection
            ids: Optional list of IDs to delete
            where: Optional metadata filter
            where_document: Optional document filter

        Returns:
            Number of items deleted
        """

        collection_name = InputValidator.validate_table_name(collection_name)

        if not self.collection_exists(collection_name):
            raise ValueError(f"Collection '{collection_name}' not found")

        cursor = self.conn.cursor()

        if ids is not None:
            placeholders = ", ".join(["?" for _ in ids])
            cursor.execute(
                f"DELETE FROM [vec_{collection_name}] WHERE id IN ({placeholders})",
                list(ids)
            )
        else:
            # Get all items and filter
            items = self._get_all_items(collection_name)
            ids_to_delete = []

            for item in items:
                if self._matches_filters(item, where, where_document):
                    ids_to_delete.append(item["id"])

            if ids_to_delete:
                placeholders = ", ".join(["?" for _ in ids_to_delete])
                cursor.execute(
                    f"DELETE FROM [vec_{collection_name}] WHERE id IN ({placeholders})",
                    ids_to_delete
                )

        deleted_count = cursor.rowcount
        self.conn.commit()
        return deleted_count


    # count items in a collection
    def count(self, collection_name: str) -> int:
        """
        Count items in a collection.

        Args:
            collection_name: Name of the collection

        Returns:
            Number of items in the collection
        """

        collection_name = InputValidator.validate_table_name(collection_name)

        if not self.collection_exists(collection_name):
            raise ValueError(f"Collection '{collection_name}' not found")

        cursor = self.conn.cursor()

        cursor.execute(f"SELECT COUNT(*) FROM [vec_{collection_name}]")
        return cursor.fetchone()[0]


    # get all items from a collection
    def _get_all_items(self, collection_name: str) -> List[Dict[str, Any]]:
        """
        Get all items from a collection.
        """

        cursor = self.conn.cursor()
        
        cursor.execute(f"SELECT * FROM [vec_{collection_name}]")

        items = []
        for row in cursor.fetchall():
            items.append({
                "id": row["id"],
                "document": row["document"],
                "embedding": json.loads(row["embedding"]),
                "metadata": json.loads(row["metadata"]) if row["metadata"] else None,
                "created_at": row["created_at"]
            })

        return items


    # check if item matches the given filters
    def _matches_filters(
        self,
        item: Dict[str, Any],
        where: Optional[Dict[str, Any]] = None,
        where_document: Optional[Dict[str, str]] = None,
    ) -> bool:
        """
        Check if an item matches the given filters.

        Args:
            item: Item to check
            where: Metadata filter
            where_document: Document filter

        Returns:
            True if item matches all filters
        """

        # Check metadata filter
        if where is not None:
            metadata = item.get("metadata") or {}

            for key, value in where.items():
                # Handle special operators
                if key.startswith("$"):
                    if key == "$and":
                        if not all(
                            self._matches_filters(item, cond, None)
                            for cond in value
                        ):
                            return False
                    elif key == "$or":
                        if not any(
                            self._matches_filters(item, cond, None)
                            for cond in value
                        ):
                            return False
                else:
                    # Handle comparison operators in value
                    if isinstance(value, dict):
                        meta_value = metadata.get(key)
                        for op, op_value in value.items():
                            if op == "$eq" and meta_value != op_value:
                                return False
                            elif op == "$ne" and meta_value == op_value:
                                return False
                            elif op == "$gt" and not (meta_value is not None and meta_value > op_value):
                                return False
                            elif op == "$gte" and not (meta_value is not None and meta_value >= op_value):
                                return False
                            elif op == "$lt" and not (meta_value is not None and meta_value < op_value):
                                return False
                            elif op == "$lte" and not (meta_value is not None and meta_value <= op_value):
                                return False
                            elif op == "$in" and meta_value not in op_value:
                                return False
                            elif op == "$nin" and meta_value in op_value:
                                return False
                    else:
                        # Simple equality check
                        if metadata.get(key) != value:
                            return False

        # Check document filter
        if where_document is not None:
            document = item.get("document") or ""

            for op, value in where_document.items():
                if op == "$contains" and value not in document:
                    return False
                elif op == "$not_contains" and value in document:
                    return False

        return True


    # set the embedding function for the database
    def set_embedding_function(
        self,
        embedding_function: Callable[[List[str]], List[List[float]]]
    ) -> None:
        """
        Set the embedding function for the database.

        Args:
            embedding_function: Function that takes texts and returns embeddings
        """

        self.embedding_function = embedding_function


    # close the database connection
    def close(self) -> None:
        """
        Close database connection.
        """

        if self.conn:
            self.conn.close()
