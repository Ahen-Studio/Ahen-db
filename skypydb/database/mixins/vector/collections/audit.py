"""
Module containing the AuditCollections class, which is used to check the integrity of a collection.
"""

from typing import (
    Optional,
    Dict,
    Any
)
from skypydb.security.validation import InputValidator

class AuditCollections:
    def collection_exists(
        self,
        name: str
    ) -> bool:
        """
        Check if a collection exists.

        Args:
            name: Collection name

        Returns:
            True if collection exists
        """

        name = InputValidator.validate_table_name(name)

        cursor = self.conn.cursor()

        cursor.execute(
            "SELECT name FROM sqlite_master WHERE type='table' AND name=?",
            (f"vec_{name}",)
        )
        return cursor.fetchone() is not None

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

    def _matches_filters(
        self,
        item: Dict[str, Any],
        where: Optional[Dict[str, Any]] = None,
        where_document: Optional[Dict[str, str]] = None
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

        # check metadata filter
        if where is not None:
            metadata = item.get("metadata") or {}
            for key, value in where.items():
                # handle special operators
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
                    # handle comparison operators in value
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
                        # simple equality check
                        if metadata.get(key) != value:
                            return False
        # check document filter
        if where_document is not None:
            document = item.get("document") or ""

            for op, value in where_document.items():
                if op == "$contains" and value not in document:
                    return False
                elif op == "$not_contains" and value in document:
                    return False
        return True