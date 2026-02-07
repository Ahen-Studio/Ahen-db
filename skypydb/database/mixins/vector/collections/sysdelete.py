"""
Module containing the SysDelete class, which is used to delete a collection.
"""

from skypydb.security.validation import InputValidator

class SysDelete:
    def delete_collection(
        self,
        name: str
    ) -> None:
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

        # drop the collection table
        table_name = f"vec_{name}"
        cursor.execute("DROP TABLE [" + table_name + "]")

        # remove from collections metadata
        cursor.execute(
            "DELETE FROM _vector_collections WHERE name = ?",
            (name,)
        )
        self.conn.commit()