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
        auto_start_dashboard: bool = True
    ):
        """
        Create a Client configured to use the given SQLite database file and optional dashboard.
        
        Creates and stores a Database instance and a dashboard thread placeholder. If auto_start_dashboard is True, the dashboard will be started in a background thread.
        
        Parameters:
            path (str): Filesystem path to the SQLite database file.
            dashboard_port (int): TCP port for the dashboard UI.
            auto_start_dashboard (bool): If True, start the dashboard in a background thread.
        """

        self.path = path
        self.dashboard_port = dashboard_port
        self.db = Database(path)
        self._dashboard_thread: Optional[threading.Thread] = None

        if auto_start_dashboard:
            self.start_dashboard()

    def create_table(self, table_name: str) -> Table:
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

    def get_table(self, table_name: str) -> Table:
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

    def delete_table(self, table_name: str) -> None:
        """
        Delete the specified table from the database and remove its associated table configuration.
        
        Raises:
            TableNotFoundError: If the table does not exist.
        """

        self.db.delete_table(table_name)

        # Also delete the configuration
        self.db.delete_table_config(table_name)

    def create_table_from_config(
        self,
        config: Dict[str, Any],
        table_name: Optional[str] = None
    ) -> Union["Table", Dict[str, "Table"]]:
        """
        Create one or more tables from a configuration mapping.
        
        Parameters:
            config (Dict[str, Any]): Mapping of table names to column definitions, e.g. {"users": {"name": "str", "id": "auto"}, ...}.
            table_name (Optional[str]): If provided, create only this table from the config.
        
        Returns:
            Table | Dict[str, Table]: A Table instance for the created table when `table_name` is provided; otherwise a dict mapping each created table name to its Table instance.
        
        Raises:
            KeyError: If `table_name` is provided but not present in `config`.
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

    def get_table_from_config(self, config: Dict[str, Any], table_name: str) -> "Table":
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

    def delete_table_from_config(self, config: Dict[str, Any], table_name: str) -> None:
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

    def start_dashboard(self) -> None:
        """
        Launches the dashboard web UI in a background thread.
        
        If a dashboard thread is already running, this is a no-op. Sets the environment
        variables SKYPYDB_PATH and SKYPYDB_PORT and starts a daemon thread that runs the
        dashboard application (via uvicorn) bound to 127.0.0.1 on the client's
        dashboard_port. Waits briefly to allow the server to start; startup exceptions
        are printed to stdout.
        """

        if self._dashboard_thread and self._dashboard_thread.is_alive():
            return  # Dashboard already running

        def run_dashboard():
            # Set environment variables before importing app
            """
            Starts the dashboard web server using the client's configured database path and port.
            
            Sets the `SKYPYDB_PATH` and `SKYPYDB_PORT` environment variables, imports the dashboard ASGI app, and runs it with Uvicorn on 127.0.0.1 at the configured port. Any exception raised during startup is caught and printed.
            """
            os.environ["SKYPYDB_PATH"] = self.path
            os.environ["SKYPYDB_PORT"] = str(self.dashboard_port)

            # Import and run the app
            from ..dashboard.dashboard.dashboard import app

            # Use uvicorn to run the app
            try:
                import uvicorn

                uvicorn.run(
                    app, host="127.0.0.1", port=self.dashboard_port, log_level="warning"
                )

            except Exception as e:
                print(f"Error starting dashboard: {e}")

        self._dashboard_thread = threading.Thread(target=run_dashboard, daemon=True)
        self._dashboard_thread.start()

        # Give the dashboard a moment to start
        time.sleep(0.5)

    def wait(self) -> None:
        """
        Block the process while the dashboard thread is running and close the client on KeyboardInterrupt.
        
        If a dashboard thread is alive, prints the dashboard URL and a prompt, then sleeps until a KeyboardInterrupt is received, at which point the client's resources are closed. If no dashboard is running, prints a message indicating how to start it.
        """
        
        if self._dashboard_thread and self._dashboard_thread.is_alive():
            # show dashboard URL
            print(f"Dashboard is running at http://127.0.0.1:{self.dashboard_port}")
            
            print("Press Ctrl+C to stop...")
            try:
                while True:
                    time.sleep(1)
            except KeyboardInterrupt:
                print("\nStopping...")
                self.close()
        else:
            print("Dashboard is not running. Start it with client.start_dashboard()")

    def close(self) -> None:
        """
        Close the client's database connection.
        
        Closes the underlying Database instance associated with this Client.
        """

        self.db.close()