"""
Cli for Skypydb.
"""

import base64
from pathlib import Path
from typing import Optional
import typer
import questionary
from rich import print
from skypydb.cli.start_dashboard import start_dashboard
from skypydb.security import EncryptionManager
from skypydb import __version__


# initialize the cli app
app = typer.Typer(
    name="skypydb",
    help="Skypydb CLI - Open Source Reactive and Vector Embedding Database"
)


# main class for Skypy Cli
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
        gitignore_path: str = ".gitignore",
        gitignore_entry: str = ".env.local",
        cwd: Path = Path.cwd(),
    ):
        """
        Initialize the CLI with configuration variables.
        """

        self.env_file_name = env_file_name
        self.skypydb_folder = skypydb_folder
        self.generated_folder = generated_folder
        self.schema_file_name = schema_file_name
        self.gitignore_path = gitignore_path
        self.gitignore_entry = gitignore_entry
        self.cwd = cwd


    # launch the dashboard
    def launch_dashboard(
        self,
        api_port: int = 8000,
        dashboard_port: int = 3000,
        db_path: Optional[str] = None,
        vector_db_path: Optional[str] = None,
    ) -> None:
        """
        Launch the dashboard.
        
        Args:
            api_port: Port for the API server
            dashboard_port: Port for the dashboard frontend
            db_path: Path to the main database file
            vector_db_path: Path to the vector database file
        """

        start_dashboard(
            api_port=api_port,
            dashboard_port=dashboard_port,
            db_path=db_path,
            vector_db_path=vector_db_path,
        )


    # initialize project with encryption keys and project structure
    def init_project(self) -> None:
        """
        Initialize project with encryption keys and project structure.
        """

        # create project structure
        self._create_project_structure()

        # generate and save encryption keys
        self._generate_encryption_keys()

        # update .gitignore
        self._update_gitignore()

        # print success messages
        self._print_success_messages()


    # create project structure
    def _create_project_structure(self) -> None:
        """
        Create the project directory structure.
        """

        # Create db folder
        skypydb_dir = self.cwd / self.skypydb_folder
        if not skypydb_dir.exists():
            skypydb_dir.mkdir(exist_ok=True)

        # Create _generated folder
        generated_dir = skypydb_dir / self.generated_folder
        if not generated_dir.exists():
            generated_dir.mkdir(exist_ok=True)

        # Create schema.py file
        schema_file = skypydb_dir / self.schema_file_name
        if not schema_file.exists():
            schema_file.write_text("", encoding="utf-8")


    # generate and save encryption keys
    def _generate_encryption_keys(self) -> None:
        """
        Generate and save encryption keys to .env.local.
        """

        # generate encryption key and encode the salt key with base64
        encryption_key = EncryptionManager.generate_key()
        salt_key = EncryptionManager.generate_salt()
        salt_b64 = base64.b64encode(salt_key).decode("utf-8")

        # save encryption key and salt key to .env.local
        env_path = self.cwd / self.env_file_name
        content = f"ENCRYPTION_KEY={encryption_key}\nSALT_KEY={salt_b64}\n"
        env_path.write_text(content, encoding="utf-8")


    # update .gitignore
    def _update_gitignore(self) -> None:
        """
        Add .env.local to .gitignore.
        """

        # generate .gitignore if not exists
        gitignore_path = self.cwd / self.gitignore_path
        content = gitignore_path.read_text(encoding="utf-8") if gitignore_path.exists() else ""

        # add .env.local to .gitignore if not exists
        if self.gitignore_entry not in content.splitlines():
            content = f"{content.rstrip()}\n{self.gitignore_entry}\n" if content else f"{self.gitignore_entry}\n"
            gitignore_path.write_text(content, encoding="utf-8")


    # print success messages
    def _print_success_messages(self) -> None:
        """
        Print the success messages after project initialization.
        """

        print("[green]✔ Initialized project[/green]")
        print("[green]✔ Provisioned encryption keys and saved its:[/green]")
        print("    encryption key as ENCRYPTION_KEY")
        print("    salt key as SALT_KEY")
        print(f" to {self.env_file_name}")
        print(f'[green]  Added "{self.env_file_name}" to {self.gitignore_path}[/green]\n')
        print(f"Write your Skypydb functions in {Path(self.skypydb_folder) / self.schema_file_name}")
        print("Give us feedback at https://github.com/Ahen-Studio/skypy-db/issues")


# start skypydb development mode
@app.command()
def dev() -> None:
    """
    Start Skypydb development mode.
    """

    cli = SkypyCLI()

    # Show welcome prompt
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


# show the skypydb version and exit
def _version_callback(value: bool) -> None:
    """
    Show the skypydb version and exit.
    """

    if value:
        print(f"skypydb {__version__}")
        raise typer.Exit()


# callback for cli app
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
    Skypydb CLI - Open Source Reactive and Vector Embedding Database.
    """

    pass


# Main loop
def main() -> None:
    """
    Main entry point for the CLI.
    """

    app()

if __name__ == "__main__":
    main()
