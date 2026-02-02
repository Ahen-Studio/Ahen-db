"""
Dashboard launcher for Skypydb.
"""

import os
import subprocess
import sys
import threading
import time
import signal
from pathlib import Path
from typing import Optional, TextIO
from urllib.request import urlopen
from urllib.error import URLError
import uvicorn
from rich import print
from skypydb.api.server import app


# main class for managing dashboard lifecycle
class DashboardManager:
    """
    Manages both API server and dashboard frontend lifecycle.
    """

    def __init__(
        self,
        api_host: str = "0.0.0.0",
        api_port: int = 8000,
        dashboard_port: int = 3000,
        db_path: Optional[str] = None,
        vector_db_path: Optional[str] = None,
    ):
        self.api_host = api_host
        self.api_port = api_port
        self.dashboard_port = dashboard_port
        self.db_path = db_path
        self.vector_db_path = vector_db_path
        self.dashboard_dir = Path(__file__).parent.parent / "dashboard"
        self.frontend_process: Optional[subprocess.Popen] = None
        self._shutdown_event = threading.Event()


    # set environment variables for database paths
    def _set_env_vars(self) -> None:
        """
        Set environment variables for database paths.
        """

        if self.db_path:
            os.environ["SKYPYDB_PATH"] = self.db_path
        if self.vector_db_path:
            os.environ["SKYPYDB_VECTOR_PATH"] = self.vector_db_path


    # start the API server
    def _start_api(self) -> None:
        """
        Start the FastAPI server.
        """

        print(f"[green]Starting API server at http://{self.api_host}:{self.api_port}[/green]")
        uvicorn.run(
            app,
            host=self.api_host,
            port=self.api_port,
            log_level="info"
        )


    # check API server health status before starting dashboard
    def _wait_for_api(self) -> bool:
        """
        Wait for API server to be ready.
        """

        health_url = f"http://localhost:{self.api_port}/api/health"
        start_time = time.time()

        while time.time() - start_time < 30:
            try:
                urlopen(health_url, timeout=2)
                print("[green]API server is ready[/green]")
                return True
            except URLError:
                time.sleep(0.5)
        return False


    # stream subprocess output to console
    def _stream_output(self, pipe: Optional[TextIO], prefix: str = "") -> None:
        """
        Stream subprocess output to console.
        """

        if pipe is None:
            return
        try:
            for line in iter(pipe.readline, ""):
                if line:
                    print(f"{prefix}{line}", end="")
                if self._shutdown_event.is_set():
                    break
        finally:
            pipe.close()


    # start the dashboard frontend server
    def _start_frontend(self) -> None:
        """
        Start the Next.js dashboard frontend.
        """

        if not self.dashboard_dir.exists():
            print(f"[red]Error: Dashboard not found at {self.dashboard_dir}[/red]")
            sys.exit(1)

        print(f"[green]Starting dashboard at http://localhost:{self.dashboard_port}[/green]")

        env = os.environ.copy()
        env["PORT"] = str(self.dashboard_port)

        try:
            self.frontend_process = subprocess.Popen(
                ["npm", "run", "dev"],
                cwd=self.dashboard_dir,
                env=env,
                stdout=subprocess.PIPE,
                stderr=subprocess.PIPE,
                text=True,
                encoding="utf-8",
                errors="replace"
            )

            threading.Thread(
                target=self._stream_output,
                args=(self.frontend_process.stdout, "[Dashboard] "),
                daemon=True
            ).start()
            threading.Thread(
                target=self._stream_output,
                args=(self.frontend_process.stderr, "[Dashboard] "),
                daemon=True
            ).start()

        except FileNotFoundError:
            print("[red]Error: npm not found. Please install Node.js.[/red]")
            sys.exit(1)
        except Exception as exc:
            print(f"[red]Error: Failed to start dashboard: {exc}[/red]")
            sys.exit(1)


    # shutdown the frontend server
    def _cleanup(self) -> None:
        """
        Terminate frontend process and exit.
        """

        print("\n[yellow]Shutting down...[/yellow]")
        self._shutdown_event.set()

        if self.frontend_process:
            self.frontend_process.terminate()
            try:
                self.frontend_process.wait(timeout=5)
            except subprocess.TimeoutExpired:
                self.frontend_process.kill()

        sys.exit(0)


    # start both backend server and frontend server
    def start_both(self) -> None:
        """
        Start both API server and dashboard frontend.
        """

        print("[bold cyan]Starting Skypydb Dashboard[/bold cyan]\n")

        signal.signal(signal.SIGINT, lambda s, f: self._cleanup())
        signal.signal(signal.SIGTERM, lambda s, f: self._cleanup())

        self._set_env_vars()

        # Start API in background thread
        threading.Thread(target=self._start_api, daemon=True).start()

        if not self._wait_for_api():
            print("[red]Error: API server failed to start[/red]")
            sys.exit(1)

        self._start_frontend()

        # Monitor and wait for shutdown
        try:
            while not self._shutdown_event.is_set():
                if self.frontend_process and self.frontend_process.poll() is not None:
                    if self.frontend_process.returncode != 0:
                        print(f"[red]Dashboard exited with code {self.frontend_process.returncode}[/red]")
                    break
                time.sleep(0.5)
        except KeyboardInterrupt:
            pass
        finally:
            self._cleanup()


# start both backend server and frontend server
def start_dashboard(
    api_host: str = "0.0.0.0",
    api_port: int = 8000,
    dashboard_port: int = 3000,
    db_path: Optional[str] = None,
    vector_db_path: Optional[str] = None,
) -> None:
    """
    Start both API server and dashboard.
    """

    DashboardManager(
        api_host=api_host,
        api_port=api_port,
        dashboard_port=dashboard_port,
        db_path=db_path,
        vector_db_path=vector_db_path,
    ).start_both()

if __name__ == "__main__":
    start_dashboard()
