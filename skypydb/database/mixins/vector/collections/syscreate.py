"""
Module containing the SysCreate class, which is used to create a new vector collection.
"""

import json
from datetime import datetime
from typing import (
    Optional,
    Dict,
    Any
)
from skypydb.security.validation import InputValidator

class SysCreate:
    def create_collection(
        self,
        name: str,
        metadata: Optional[Dict[str, Any]] = None
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
        table_name = f"vec_{name}"
        if self.collection_exists(name):
            raise ValueError(f"Collection '{name}' already exists")

        cursor = self.conn.cursor()

        # create the collection table
        cursor.execute(f"""
            CREATE TABLE [{table_name}] (
                id TEXT PRIMARY KEY,
                document TEXT,
                embedding TEXT NOT NULL,
                metadata TEXT,
                created_at TEXT NOT NULL
            )
        """)

        # store collection metadata
        cursor.execute(
            "INSERT INTO _vector_collections (name, metadata, created_at) VALUES (?, ?, ?)",
            (name, json.dumps(metadata or {}), datetime.now().isoformat())
        )
        self.conn.commit()