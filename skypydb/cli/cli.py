"""
Cli for Skypydb using Typer.

Commands:
- init: Initialize project with encryption keys and project structure
- launch: Launch the dashboard
- dev: Interactive menu to choose actions
"""

import base64
import os
from pathlib import Path
import typer
import questionary
from rich import print
from ..security import EncryptionManager
from .. import __version__


# Initialize Typer app
app = typer.Typer(
    name="Skypydb Cli - Open Source Reactive Database"
)


# main class for Skypy Cli
class SkypyCLI:
    """
    Skypy CLI class to manage CLI operations.
    """

    def __init__(
        self,
        env_file_name: str = ".env.local",
        skypydb_folder: str = "skypydb",
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


    # clear the terminal screen
    def clear_screen(
        self,
    ) -> None:
        """
        Clear the terminal screen.
        """

        os.system("cls" if os.name == "nt" else "clear")


    # get user choice for the menu
    def get_user_choice(
        self,
        options: list[tuple[str, str]],
    ) -> str:
        """
        Get user choice.

        Args:
            options: List of (key, description) tuples
 
        Returns:
            The selected key
        """

        self.clear_screen()

        choices = [
            questionary.Choice(title=description, value=key)
            for key, description in options
        ]

        selection = questionary.select(
            "What would you like to do?",
            choices=choices,
            qmark="?",
            pointer="❯",
        ).ask()

        if selection is None:
            print("\n[yellow]Exiting.[/yellow]")
            raise typer.Exit(code=0)

        return selection


    # create project structure
    def create_project_structure(
        self,
    ) -> None:
        """
        Create the project directory structure.
        """

        # Create skypydb folder if it doesn't exist
        skypydb_dir = self.cwd / self.skypydb_folder

        if not skypydb_dir.exists():
            skypydb_dir.mkdir(exist_ok=True)
            print(f"[green]✓ Created {self.skypydb_folder}/[/green]")
        else:
            print(f"[yellow]→ {self.skypydb_folder}/ already exists[/yellow]")
        
        # Create schema.py file if it doesn't exist
        schema_file = skypydb_dir / self.schema_file_name

        if not schema_file.exists():
            schema_file.write_text("", encoding="utf-8")
            print(f"[green]✓ Created {self.skypydb_folder}/{self.schema_file_name}[/green]")
        else:
            print(f"[yellow]→ {self.skypydb_folder}/{self.schema_file_name} already exists[/yellow]")

        # Create _generated folder if it doesn't exist
        generated_dir = skypydb_dir / self.generated_folder

        if not generated_dir.exists():
            generated_dir.mkdir(exist_ok=True)
            print(f"[green]✓ Created {self.skypydb_folder}/{self.generated_folder}/[/green]")
        else:
            print(f"[yellow]→ {self.skypydb_folder}/{self.generated_folder}/ already exists[/yellow]")

        # Update .gitignore if it exists, otherwise create it
        gitignore_path = self.cwd / self.gitignore_path
        gitignore_entry = self.gitignore_entry

        gitignore_exists = gitignore_path.exists()
        gitignore_content = (
            gitignore_path.read_text(encoding="utf-8") if gitignore_exists else ""
        )

        if gitignore_entry not in gitignore_content.splitlines():
            if gitignore_content and not gitignore_content.endswith("\n"):
                gitignore_content += "\n"
            gitignore_content += gitignore_entry + "\n"
            gitignore_path.write_text(gitignore_content, encoding="utf-8")
            action = "Updated" if gitignore_exists else "Created"
            print(f"[green]✓ {action} .gitignore with {gitignore_entry}[/green]")


    # Initialize project with encryption keys and project structure.
    def init_project(
        self,
        overwrite: bool = False,
    ) -> None:
        """
        Initialize project with encryption keys and project structure.
        
        Args:
            overwrite: Whether to overwrite existing files
        """
        
        self.clear_screen()
        
        print("[bold cyan]Initializing Skypydb project.[/bold cyan]\n")
        
        # Create project structure
        self.create_project_structure()
        
        # Generate encryption keys
        encryption_key = EncryptionManager.generate_key()
        salt_key = EncryptionManager.generate_salt()
        # encode salt key into a string
        salt_b64 = base64.b64encode(salt_key).decode("utf-8")
        
        env_path = self.cwd / self.env_file_name
        
        # Check if .env.local already exists
        if env_path.exists() and not overwrite:
            print(f"\n[yellow]'{self.env_file_name}' already exists.[/yellow]")
            overwrite = typer.confirm("Do you want to overwrite it?", default=False)
            if not overwrite:
                print("[yellow]✗ Initialization cancelled.[/yellow]")
                return
        
        # Write content into the .env.local file
        content = ("ENCRYPTION_KEY=" + encryption_key + "\n" + "SALT_KEY=" + salt_b64 + "\n")
        env_path.write_text(content, encoding="utf-8")

        print(f"[green]✓ Created {self.env_file_name} with ENCRYPTION_KEY and SALT_KEY[/green]")
        print("\n[bold green]✓ Your project is now ready![/bold green]")


    # launch the dashboard
    def launch_dashboard(
        self,
        port: int = typer.Option(
            3000,
            "--port",
            "-p",
            help="Port for the dashboard",
        ),
        path: str = typer.Option(
            None,
            "--path",
            help="Path to the database",
        ),
    ) -> None:
        """
        Launch the Skypydb dashboard.

        Args:
            port: Port number for the dashboard
            path: Path to the database file
        """

        print("[bold cyan]Launching Skypydb dashboard.[/bold cyan]\n")

        # Set environment variables for dashboard
        os.environ["SKYPYDB_PATH"] = path
        os.environ["SKYPYDB_PORT"] = str(port)

        from ..dashboard.dashboard.dashboard import app

        try:
            import uvicorn
        except Exception as exc:
            print(f"[red]Error: Uvicorn is required to run the dashboard: {exc}[/red]")
            raise typer.Exit(code=1)

        print(f"[green]Dashboard is running at [bold]http://127.0.0.1:{port}[/bold][/green]")

        # set config for dashboard
        config = uvicorn.Config(
            app,
            host="127.0.0.1",
            port=port,
            log_level="warning"
        )
        server = uvicorn.Server(config)
        server.run()


# Typer commands
# Command to launch the dashboard and initialize a new project
@app.command()
def dev() -> None:
    """
    Show interactive menu.
    """
    
    # Create CLI instance
    cli = SkypyCLI()
    
    # Clear screen before displaying menu
    cli.clear_screen()
    
    menu_options = [
        ("init", "Initialize project"),
        ("launch", "Launch dashboard"),
        ("exit", "Exit"),
    ]
    
    while True:
        # Retrieve the user's choice
        choice = cli.get_user_choice(menu_options)
        
        # Handle user choice
        if choice == "init":
            cli.init_project()
            typer.pause()
        elif choice == "launch":
            cli.launch_dashboard()
        elif choice == "exit":
            cli.clear_screen()
            print("[bold cyan]Goodbye![/bold cyan]")
            break


# Command to initialize a new Skypydb project
@app.command()
def init(
    overwrite: bool = typer.Option(
        False,
        "--overwrite",
        "-o",
        help="Overwrite existing .env.local file",
    ),
) -> None:
    """
    Initialize a new Skypydb project with encryption keys and project structure.
    
    This command will create:
    - skypydb/ directory with schema.py file
    - _generated/ directory
    - .env.local file with ENCRYPTION_KEY and SALT_KEY
    - Update .gitignore with .env.local if it exists, otherwise create it
    """
    
    cli = SkypyCLI()
    cli.init_project(overwrite=overwrite)
    typer.pause()


# Command to launch the dashboard
@app.command()
def launch(
    port: int = typer.Option(
        3000,
        "--port",
        "-p",
        help="Port for the dashboard",
    ),
    path: str = typer.Option(
        None,
        "--path",
        help="Path to the database",
    ),
) -> None:
    """
    Launch the Skypydb dashboard.
    
    Args:
        port: Port number for the dashboard (default: 3000)
        path: Path to the database file
    """
    
    cli = SkypyCLI()
    cli.launch_dashboard(port=port, path=path)


# show the skypydb version
def _version_callback(
    value: bool
) -> None:
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
        callback=_version_callback,
    ),
) -> None:
    """
    Skypydb Cli.
    """
    
    return


# Main loop
def main() -> None:
    """
    Main entry point for the CLI.
    """
    
    app()


if __name__ == "__main__":
    main()
