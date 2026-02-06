"""
Cli for Skypydb.
"""

from io import BytesIO
from pathlib import Path
from urllib.error import (
    HTTPError,
    URLError
)
from urllib.request import (
    Request,
    urlopen
)
from zipfile import ZipFile
from pathlib import PurePosixPath
import subprocess
import shutil
import typer
import questionary
from rich import print
from skypydb.security import EncryptionManager
from skypydb import __version__

app = typer.Typer(
    name="skypydb",
    help="Skypydb CLI - Open Source Reactive and Vector Embedding Database"
)

class SkypyCLI:
    """
    Skypy CLI class to manage CLI operations.
    """

    def __init__(
        self,
        env_file_name: str = ".env.local",
        skypydb_folder: str = "db",
        generated_folder: str = "_generated",
        schema_file_name: str = "schema.py",
        repo_zip_url: str = "https://github.com/Ahen-Studio/skypy-db/archive/refs/heads/main.zip",
        repo_dashboard_path: str = "dashboard",
        gitignore_path: str = ".gitignore",
        gitignore_entry: str = ".env.local",
        cwd: Path = Path.cwd()
    ):
        """
        Initialize the CLI with configuration variables.
        """

        self.env_file_name = env_file_name
        self.skypydb_folder = skypydb_folder
        self.generated_folder = generated_folder
        self.schema_file_name = schema_file_name
        self.repo_zip_url = repo_zip_url
        self.repo_dashboard_path = repo_dashboard_path.strip("/\\")
        self.gitignore_path = gitignore_path
        self.gitignore_entry = gitignore_entry
        self.cwd = cwd

    def launch_dashboard(
        self,
        api_port: int = 8000,
        dashboard_port: int = 3000
    ) -> None:
        """
        Launch the FastAPI server and the dashboard dev server.
        """

        dashboard_dir = self.cwd / self.skypydb_folder / self.generated_folder / self.repo_dashboard_path
        if not dashboard_dir.exists():
            print(
                f"[yellow]Dashboard folder not found at {dashboard_dir}. "
                "Run project initialization first.[/yellow]"
            )
            return

        api_command = [
            "python",
            "-c",
            (
                "import uvicorn; "
                "import skypydb.server.fastapi.server as server; "
                f"uvicorn.run(server.app, host='0.0.0.0', port={api_port})"
            ),
        ]

        npm_path = shutil.which("npm") or shutil.which("npm.cmd")
        if not npm_path:
            print("[yellow]npm was not found in PATH. Please install Node.js/npm and try again.[/yellow]")
            return

        dashboard_command = [
            npm_path,
            "run",
            "dev",
            "--",
            "--port",
            str(dashboard_port)
        ]

        print(f"[green]Starting API server on port {api_port}[/green]")
        api_process = subprocess.Popen(
            api_command,
            cwd=str(self.cwd)
        )

        try:
            next_bin = dashboard_dir / "node_modules" / ".bin" / "next.cmd"
            if not next_bin.exists():
                print("[yellow]Installing dashboard dependencies (npm install).[/yellow]")
                install_process = subprocess.Popen(
                    [npm_path,
                    "install"
                ], cwd=str(dashboard_dir))
                install_process.wait()

            print(f"[green]Starting dashboard on port {dashboard_port}[/green]")
            dashboard_process = subprocess.Popen(
                dashboard_command,
                cwd=str(dashboard_dir)
            )
        except FileNotFoundError as exc:
            print(f"[yellow]Failed to start dashboard: {exc}[/yellow]")
            api_process.terminate()
            return

        try:
            api_process.wait()
            dashboard_process.wait()
        except KeyboardInterrupt:
            print("\n[yellow]Shutting down servers.[/yellow]")
        finally:
            api_process.terminate()
            dashboard_process.terminate()

    def init_project(self) -> None:
        """
        Initialize project with encryption keys and project structure.
        """

        # create project structure
        self._create_project_structure()

        # generate, save encryption keys and update .gitignore
        self._generate_encryption_keys_and_update_gitignore()

        # download the dashboard folder from GitHub
        self._download_dashboard_folder()

        print(f"Write your Skypydb functions in {Path(self.skypydb_folder) / self.schema_file_name}")
        print("Give us feedback at https://github.com/Ahen-Studio/skypy-db/issues")

    def _create_project_structure(self) -> None:
        """
        Create the project directory structure.
        """

        # create db folder
        skypydb_dir = self.cwd / self.skypydb_folder
        if not skypydb_dir.exists():
            skypydb_dir.mkdir(exist_ok=True)

        # create _generated folder
        generated_dir = skypydb_dir / self.generated_folder
        if not generated_dir.exists():
            generated_dir.mkdir(exist_ok=True)

        # create schema.py file
        schema_file = skypydb_dir / self.schema_file_name
        if not schema_file.exists():
            schema_file.write_text("", encoding="utf-8")

        print("[green]✔ Initialized project[/green]")

    def _generate_encryption_keys_and_update_gitignore(self) -> None:
        """
        Generate and save encryption keys to .env.local and update the gitignore file to untrack the .env.local file.
        """

        # generate encryption key and encode the salt key with base64
        encryption_key = EncryptionManager.generate_key()
        salt_key = EncryptionManager.generate_salt()

        # save encryption key and salt key to .env.local
        env_path = self.cwd / self.env_file_name
        content = f"ENCRYPTION_KEY={encryption_key}\nSALT_KEY={salt_key}\n"
        env_path.write_text(content, encoding="utf-8")

        # generate .gitignore if not exists
        gitignore_path = self.cwd / self.gitignore_path
        content = gitignore_path.read_text(encoding="utf-8") if gitignore_path.exists() else ""

        # add .env.local to .gitignore if not exists
        if self.gitignore_entry not in content.splitlines():
            content = f"{content.rstrip()}\n{self.gitignore_entry}\n" if content else f"{self.gitignore_entry}\n"
            gitignore_path.write_text(content, encoding="utf-8")

        print("[green]✔ Encryption keys provided and saved:[/green]")
        print("    encryption key as ENCRYPTION_KEY")
        print("    salt key as SALT_KEY")
        print(f"to {self.env_file_name}")
        print(f'[bold bright_black]Added "{self.env_file_name}" to {self.gitignore_path}[/bold bright_black]\n')

    def _download_dashboard_folder(self) -> None:
        """
        Download the dashboard folder from the GitHub repo zip into the generated directory.
        """

        if not self.repo_zip_url or not self.repo_dashboard_path:
            return

        generated_dir = self.cwd / self.skypydb_folder / self.generated_folder
        target_root = generated_dir / self.repo_dashboard_path

        try:
            request = Request(
                self.repo_zip_url,
                headers={"User-Agent": "skypydb-cli"}
            )
            with urlopen(request) as response:
                zip_bytes = BytesIO(response.read())
        except (HTTPError, URLError) as exc:
            print(f"[yellow]Warning: failed to download dashboard folder. {exc}[/yellow]")
            return

        created_files = 0
        skipped_files = 0

        with ZipFile(zip_bytes) as archive:
            names = archive.namelist()
            repo_root = names[0].split("/")[0] if names else None
            if not repo_root:
                print("[yellow]Warning: unexpected zip format (missing root).[/yellow]")
                return
            prefix = f"{repo_root}/{self.repo_dashboard_path}/"
            for info in archive.infolist():
                if not info.filename.startswith(prefix):
                    continue
                relative = info.filename[len(prefix):]
                if not relative:
                    continue
                rel_path = PurePosixPath(relative)
                if ".." in rel_path.parts:
                    continue
                target_path = target_root / Path(*rel_path.parts)
                if info.is_dir():
                    target_path.mkdir(parents=True, exist_ok=True)
                elif target_path.exists():
                    skipped_files += 1
                else:
                    target_path.parent.mkdir(parents=True, exist_ok=True)
                    with archive.open(info) as src:
                        target_path.write_bytes(src.read())
                    created_files += 1
        if created_files:
            print(f"[green]✔ Downloaded dashboard folder to {target_root}[/green]")
        if skipped_files:
            print(f"[yellow]Skipped {skipped_files} existing file(s) in {target_root}[/yellow]")

@app.command()
def dev() -> None:
    """
    Start Skypydb interactive mode.
    """

    cli = SkypyCLI()

    choices = [
        questionary.Choice(title="create a new project", value="create"),
        questionary.Choice(title="launch the dashboard", value="dashboard"),
        questionary.Choice(title="no thanks", value="exit"),
    ]

    selection = questionary.select(
        "Welcome to Skypydb! Would you like to create a new project?",
        choices=choices,
        qmark="?",
        pointer="❯",
    ).ask()

    if selection is None or selection == "exit":
        print("\n[yellow]Exiting.[/yellow]")
        raise typer.Exit(code=0)
    if selection == "create":
        cli.init_project()
    elif selection == "dashboard":
        cli.launch_dashboard()

def _version_callback(value: bool) -> None:
    """
    Show the skypydb version and exit.
    """

    if value:
        print(f"skypydb {__version__}")
        raise typer.Exit()

@app.callback()
def main_callback(
    version: bool = typer.Option(
        False,
        "--version",
        help="Show version and exit",
        is_eager=True,
        callback=_version_callback
    )
) -> None:
    """
    Main cli callback.
    """

    pass

def main() -> None:
    """
    Main entry point for the CLI.
    """

    app()

if __name__ == "__main__":
    main()
