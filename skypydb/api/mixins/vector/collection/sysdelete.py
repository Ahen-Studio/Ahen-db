"""
Module containing the SysDelete class, which is used to delete items from a collection.
"""

from typing import (
    Any,
    Dict,
    Optional,
    List
)

class SysDelete:
    def delete(
        self,
        ids: Optional[List[str]] = None,
        where: Optional[Dict[str, Any]] = None,
        where_document: Optional[Dict[str, str]] = None
    ) -> None:
        """
        Delete items from the collection.

        Args:
            ids: Optional list of IDs to delete
            where: Optional metadata filter
            where_document: Optional document content filter

        Example:
            # Delete by ID
            collection.delete(ids=["doc1", "doc2"])

            # Delete by metadata filter
            collection.delete(where={"source": "old"})

            # Delete by document content
            collection.delete(where_document={"$contains": "deprecated"})
        """

        # require at least one selector to avoid accidental deletion of all items.
        if ids is None and where is None and where_document is None:
            raise ValueError(
                "delete() requires at least one of 'ids', 'where', or "
                "'where_document' to be provided."
            )

        self._db.delete(
            collection_name=self._name,
            ids=ids,
            where=where,
            where_document=where_document
        )