"""
Module containing the SysAdd class, which is used to get information about the collection.
"""

import json
from typing import (
    List,
    Optional,
    Dict,
    Any
)
from skypydb.security.validation import InputValidator

class SysGet:
    def get_collection(
        self,
        name: str
    ) -> Optional[Dict[str, Any]]:
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

    def get_or_create_collection(
        self,
        name: str,
        metadata: Optional[Dict[str, Any]] = None
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
        # at this point collection must exist since we just created it if needed
        assert result is not None
        return result

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