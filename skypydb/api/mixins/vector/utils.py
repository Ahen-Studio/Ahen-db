"""
Module containing the Utils class, which is used to check and reset the database.
"""

import time

class Utils:
    def reset(
        self
    ) -> bool:
        """
        Reset the database by deleting all collections.

        Returns:
            True if reset was successful

        Example:
            client.reset()
        """

        # prefer a single backend operation if available
        reset_method = None
        if hasattr(self._db, "reset"):
            reset_method = getattr(self._db, "reset")
        elif hasattr(self._db, "clear"):
            reset_method = getattr(self._db, "clear")
        if callable(reset_method):
            reset_method()
        else:
            for collection_info in self._db.list_collections():
                self._db.delete_collection(collection_info["name"])

        self._collections.clear()
        return True

    def heartbeat(
        self
    ) -> int:
        """
        Check if the database is alive.

        Returns:
            Current timestamp in nanoseconds

        Example:
            if client.heartbeat():
                print("Database is alive")
        """

        return int(time.time() * 1e9)