"""
Module containing the SysDelete class, which is used to delete a collection.
"""

class SysDelete:
    def delete_collection(
        self,
        name: str
    ) -> None:
        """
        Delete a collection and all its data.

        This permanently removes the collection and all documents,
        embeddings, and metadata stored within it.

        Args:
            name: Name of the collection to delete

        Raises:
            ValueError: If collection doesn't exist

        Example:
            client.delete_collection("old-articles")
        """

        # delete from database
        self._db.delete_collection(name)
        # remove from cache
        if name in self._collections:
            del self._collections[name]