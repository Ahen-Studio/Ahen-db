"""
Client API for SkypyDB.
"""

import threading
import time
import os
from typing import Any, Dict, Optional, Union

from ..db.database import Database
from ..errors import TableAlreadyExistsError, TableNotFoundError
from ..table.table import Table


class Client:
    """
    Main client for interacting with SkypyDB.
    """

    def __init__(
        self,
        path: str,
        dashboard_port: int = 3000,
        auto_start_dashboard: bool = True,
    ):
        """
        Initialize SkypyDB client.

        Args:
            path: Path to SQLite database file
            dashboard_port: Port for the dashboard (default: 3000)
            auto_start_dashboard: Whether to automatically start dashboard (non-blocking)
        """

        self.path = path
        self.dashboard_port = dashboard_port
        self.db = Database(path)
        self._dashboard_thread: Optional[threading.Thread] = None
        self._dashboard_server = None

        if auto_start_dashboard:
            self.start_dashboard(block=False)

    def create_table(
        self,
        table_name: str,
    ) -> Table:
        """
        Create a new table.

        Args:
            table_name: Name of the table to create

        Returns:
            Table instance

        Raises:
            TableAlreadyExistsError: If table already exists
        """

        if self.db.table_exists(table_name):
            raise TableAlreadyExistsError(f"Table '{table_name}' already exists")

        self.db.create_table(table_name)
        return Table(self.db, table_name)

    def get_table(
        self,
        table_name: str,
    ) -> Table:
        """
        Get an existing table.

        Args:
            table_name: Name of the table

        Returns:
            Table instance

        Raises:
            TableNotFoundError: If table doesn't exist
        """

        if not self.db.table_exists(table_name):
            raise TableNotFoundError(f"Table '{table_name}' not found")
        return Table(self.db, table_name)

    def delete_table(
        self,
        table_name: str,
    ) -> None:
        """
        Delete a table.

        Args:
            table_name: Name of the table to delete

        Raises:
            TableNotFoundError: If table doesn't exist
        """

        self.db.delete_table(table_name)

        # Also delete the configuration
        self.db.delete_table_config(table_name)

    def create_table_from_config(
        self,
        config: Dict[str, Any],
        table_name: Optional[str] = None,
    ) -> Union["Table", Dict[str, "Table"]]:
        """
        Create table(s) from configuration.

        Args:
            config: Configuration dictionary with table definitions
                    Format: {"table_name": {"col1": "str", "col2": "int", "id": "auto"}, ...}
            table_name: If provided, only create this specific table from the config

        Returns:
            Table instance if table_name is provided, otherwise dictionary of table_name -> Table

        Example:
            config = {
                "users": {
                    "name": "str",
                    "email": "str",
                    "age": int,
                    "id": "auto"
                },
                "posts": {
                    "title": "str",
                    "content": "str",
                    "id": "auto"
                }
            }

            # Create all tables
            table = client.create_table_from_config(config)

            # Create only 'users' table
            table = client.create_table_from_config(config, table_name="users")
        """

        if table_name is not None:
            # Create single table
            if table_name not in config:
                raise KeyError(f"Table '{table_name}' not found in config")

            table_config = config[table_name]
            self.db.create_table_from_config(table_name, table_config)
            return Table(self.db, table_name)
        else:
            # Create all tables
            table = {}
            for name, table_config in config.items():
                self.db.create_table_from_config(name, table_config)
                table[name] = Table(self.db, name)
            return table

    def get_table_from_config(
        self,
        config: Dict[str, Any],
        table_name: str,
    ) -> "Table":
        """
        Get a table instance from configuration.

        This method retrieves an existing table. It doesn't create the table if it doesn't exist.

        Args:
            config: Configuration dictionary (for reference/validation)
            table_name: Name of the table to retrieve

        Returns:
            Table instance

        Raises:
            TableNotFoundError: If table doesn't exist

        Example:
            config = {
                "users": {
                    "name": "str",
                    "email": "str"
                }
            }
            table = client.get_table_from_config(config, "users")
        """
        if not self.db.table_exists(table_name):
            raise TableNotFoundError(f"Table '{table_name}' not found")

        return Table(self.db, table_name)

    def delete_table_from_config(
        self,
        config: Dict[str, Any],
        table_name: str,
    ) -> None:
        """
        Delete a table and its configuration.

        Args:
            config: Configuration dictionary (for reference)
            table_name: Name of the table to delete

        Raises:
            TableNotFoundError: If table doesn't exist

        Example:
            config = {
                "users": {
                    "name": "str",
                    "email": "str"
                }
            }
            client.delete_table_from_config(config, "users")
        """

        if table_name not in config:
            raise KeyError(f"Table '{table_name}' not found in config")

        if not self.db.table_exists(table_name):
            raise TableNotFoundError(f"Table '{table_name}' not found")

        self.db.delete_table(table_name)
        self.db.delete_table_config(table_name)

    def start_dashboard(
        self,
        block: bool = True,
    ) -> None:
        """
        Start the dashboard in a separate thread.
        
        Args:
            block: If True, blocks the main thread keeping the dashboard alive.
                   If False, returns immediately after starting the dashboard thread.
        
        When block=True, press Ctrl+C to stop.
        
        Example:
            # Non-blocking start (returns immediately)
            client = skypydb.Client(path="./data/skypy.db", auto_start_dashboard=True)
            
            try:
                table = client.create_table("all-my-documents")
            except TableAlreadyExistsError:
                # Tables already exist, that's fine
                pass
            
            # Retrieve the table before adding any data.
            table = client.get_table("all-my-documents")
            
            table.add(
                title=["document"],
                user_id=["user123"],
                content=["this is a document"],
                id=["auto"]# ids are automatically created by the backend
            )
            
            # Or explicit non-blocking start
            client = skypydb.Client(path="./data/skypy.db", auto_start_dashboard=False)
            client.start_dashboard(block=False)
            
            # Blocking start (keeps dashboard running)
            client.start_dashboard(block=True)  # or just client.start_dashboard()
        """

        if self._dashboard_thread and self._dashboard_thread.is_alive():
            # Dashboard already running
            if not block:
                # Just return if non-blocking
                return
            
            # Keep the program running so the dashboard stays active
            print(f"Dashboard is already running at http://127.0.0.1:{self.dashboard_port}")# show dashboard URL
            
            print("Press Ctrl+C to stop...")
            try:
                while True:
                    time.sleep(1)
            except KeyboardInterrupt:
                print("\nStopping...")
                self.close()
            return

        def run_dashboard():
            # Set environment variables before importing app
            os.environ["SKYPYDB_PATH"] = self.path
            os.environ["SKYPYDB_PORT"] = str(self.dashboard_port)

            # Import and run the app
            from ..dashboard.dashboard.dashboard import app

            # Use uvicorn to run the app
            try:
                import uvicorn

                config = uvicorn.Config(
                    app, host="127.0.0.1", port=self.dashboard_port, log_level="warning"
                )
                server = uvicorn.Server(config)
                self._dashboard_server = server
                server.run()

            except Exception as e:
                print(f"Error starting dashboard: {e}")

        self._dashboard_thread = threading.Thread(target=run_dashboard, daemon=True)
        self._dashboard_thread.start()
        
        # Give the dashboard a moment to start
        time.sleep(0.5)

        # Keep the program running so the dashboard stays active
        print(f"Dashboard is running at http://127.0.0.1:{self.dashboard_port}")# show dashboard URL
        
        if block:
            print("Press Ctrl+C to stop...")
            try:
                while True:
                    time.sleep(1)
            except KeyboardInterrupt:
                print("\nStopping...")
                self.close()

    def stop_dashboard(
        self,
    ) -> None:
        """
        Stop the dashboard.
        
        This will attempt to shutdown the uvicorn server and wait for the dashboard thread to exit.
        """
        
        if self._dashboard_server is not None:
            # Signal the server to shut down
            self._dashboard_server.should_exit = True
            
        if self._dashboard_thread and self._dashboard_thread.is_alive():
            # Wait for thread to finish (with timeout)
            self._dashboard_thread.join(timeout=5.0)
            
        print("Dashboard stopped.")

    def close(
        self,
    ) -> None:
        """
        Close database connection.
        """

        self.db.close()
