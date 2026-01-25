"""
Cli for Skypydb using Typer.

Commands:
- init: Initialize project with encryption keys and project structure
- launch: Launch the dashboard
- dev: Interactive menu to choose actions (Windows only)
"""

import base64
import os
from pathlib import Path

import msvcrt
import typer
from rich.panel import Panel
from rich import print

from ..security import EncryptionManager


# Initialize Typer app
app = typer.Typer(
    name="Skypydb Cli - Open Source Reactive Database"
)

# Constants
ENV_FILE_NAME = ".env.local"
SKYPYDB_FOLDER = "skypydb"
GENERATED_FOLDER = "_generated"
DB_FILE_NAME = "skypydb.db"
SCHEMA_FILE_NAME = "schema.py"


# Clear the screen
def clear_screen() -> None:
    """
    Clear the terminal screen.
    """
    
    os.system("cls")


def get_user_choice(options: list[tuple[str, str]]) -> str:
    """
    Get user choice.
    
    Args:
        options: List of (key, description) tuples
        
    Returns:
        The selected key
    """
    selected_index = 0
    
    while True:
        clear_screen()
        
        # Display menu
        lines = ["[bold cyan]? What would you like to do?[/bold cyan]"]
        for idx, (key, description) in enumerate(options):
            if idx == selected_index:
                lines.append(f"[bold green]❯ {description}[/bold green]")
            else:
                lines.append(f"{description}")
        
        menu_text = "\n".join(lines)
        print(Panel(menu_text, title="[bold]Skypydb Menu[/bold]", border_style="cyan"))
        
        key = msvcrt.getch()
        
        if key == b'\r':  # Enter key
            return options[selected_index][0]
        elif key == b'\xe0':  # Special key (arrow keys)
            arrow = msvcrt.getch()
            if arrow == b'H':  # Up arrow
                selected_index = (selected_index - 1) % len(options)
            elif arrow == b'P':  # Down arrow
                selected_index = (selected_index + 1) % len(options)


def create_project_structure() -> None:
    """
    Create the project directory structure.
    """
    
    cwd = Path.cwd()
    
    # Create skypydb folder if it doesn't exist
    skypydb_dir = cwd / SKYPYDB_FOLDER
    skypydb_dir.mkdir(exist_ok=True)
    
    # Create schema.py file if it doesn't exist
    schema_file = skypydb_dir / SCHEMA_FILE_NAME
    if not schema_file.exists():
        schema_file.write_text("", encoding="utf-8")
        print(f"[green]✓ Created {SKYPYDB_FOLDER}/{SCHEMA_FILE_NAME}[/green]")
    else:
        print(f"[yellow]→ {SKYPYDB_FOLDER}/{SCHEMA_FILE_NAME} already exists[/yellow]")
    
    # Create _generated folder if it doesn't exist
    generated_dir = cwd / GENERATED_FOLDER
    generated_dir.mkdir(exist_ok=True)
    
    
def init_project(overwrite: bool = False) -> None:
    """
    Initialize project with encryption keys and project structure.
    
    Args:
        overwrite: Whether to overwrite existing .env.local file
    """
    
    clear_screen()
    
    print("[bold cyan]Initializing Skypydb project.[/bold cyan]\n")
    
    # Create project structure
    create_project_structure()
    
    # Generate encryption keys
    encryption_key = EncryptionManager.generate_key()
    salt_key = EncryptionManager.generate_salt()
    salt_b64 = base64.b64encode(salt_key).decode("utf-8")
    
    cwd = Path.cwd()
    env_path = cwd / ENV_FILE_NAME
    
    # Check if .env.local already exists
    if env_path.exists() and not overwrite:
        print(f"\n[yellow]'{ENV_FILE_NAME}' already exists.[/yellow]")
        overwrite = typer.confirm("Do you want to overwrite it?", default=False)
        if not overwrite:
            print("[yellow]✗ Initialization cancelled.[/yellow]")
            return
    
    # Write .env.local file
    content = (
        "ENCRYPTION_KEY="
        + encryption_key
        + "\n"
        + "SALT_KEY="
        + salt_b64
        + "\n"
    )
    env_path.write_text(content, encoding="utf-8")
    print(f"[green]✓ Created {ENV_FILE_NAME} with ENCRYPTION_KEY and SALT_KEY[/green]")
    
    # Update .gitignore if it exists, otherwise create it
    gitignore_path = cwd / ".gitignore"
    gitignore_entry = ".env.local"
    
    if gitignore_path.exists():
        gitignore_content = gitignore_path.read_text(encoding="utf-8")
        if gitignore_entry not in gitignore_content.splitlines():
            if gitignore_content and not gitignore_content.endswith("\n"):
                gitignore_content += "\n"
            gitignore_content += gitignore_entry + "\n"
            gitignore_path.write_text(gitignore_content, encoding="utf-8")
            print(f"  [green]✓[/green] Updated .gitignore with {gitignore_entry}")
    else:
        gitignore_path.write_text(gitignore_entry + "\n", encoding="utf-8")
        print(f"  [green]✓[/green] Created .gitignore with {gitignore_entry}")
    
    print("\n[bold green]✓ Your project is now ready![/bold green]")


def launch_dashboard(
    port: int = typer.Option(3000, "--port", "-p", help="Port for the dashboard"),
    path: str = typer.Option("./skypydb/skypy.db", "--path", help="Path to the database"),
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
    print("\n[yellow]Press Ctrl+C to stop the server[/yellow]\n")
    
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
    
    # Clear screen before displaying menu
    clear_screen()
    
    menu_options = [
        ("init", "Initialize project"),
        ("launch", "Launch dashboard"),
        ("exit", "Exit"),
    ]
    
    while True:
        # Retrieve the user's choice
        choice = get_user_choice(menu_options)
        
        # Handle user choice
        if choice == "init":
            init_project()
            typer.pause()
        elif choice == "launch":
            launch_dashboard()
        elif choice == "exit":
            clear_screen()
            print("[bold cyan]Goodbye![/bold cyan]")
            break


# Command to initialize a new Skypydb project
@app.command()
def init(
    overwrite: bool = typer.Option(False, "--overwrite", "-o", help="Overwrite existing .env.local file"),
) -> None:
    """
    Initialize a new Skypydb project with encryption keys and project structure.
    
    This command will create:
    - skypydb/ directory with schema.py file
    - _generated/ directory with skypydb.db file
    - .env.local file with ENCRYPTION_KEY and SALT_KEY
    - Update .gitignore with .env.local if it exists, otherwise create it
    """
    
    init_project(overwrite=overwrite)
    typer.pause()


# Command to launch the dashboard
@app.command()
def launch(
    port: int = typer.Option(3000, "--port", "-p", help="Port for the dashboard"),
    path: str = typer.Option("./skypydb/skypy.db", "--path", help="Path to the database"),
) -> None:
    """
    Launch the Skypydb dashboard.
    
    Args:
        port: Port number for the dashboard (default: 3000)
        path: Path to the database file (default: ./skypydb/skypy.db)
    """
    
    launch_dashboard(port=port, path=path)


# Main loop
def main() -> None:
    """
    Main entry point for the CLI.
    """
    
    app()


if __name__ == "__main__":
    main()
