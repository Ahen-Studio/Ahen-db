"""
CLI for SkypyDB.

Commands:
- dev: Interactive menu to choose actions
"""

from __future__ import annotations

import argparse
import base64
import os
import sys
from typing import Optional

from ..security import EncryptionManager


ENV_FILE_NAME = ".env.local"

class InteractiveMenu:
    """
    Interactive menu with keyboard navigation.
    """

    def __init__(
        self,
        options: list[tuple[str, str]],
        dashboard_port: int = 3000,
        dashboard_path: str = "./data/skypy.db",
    ):
        self.dashboard_port = dashboard_port
        self.path = dashboard_path
        self.options = options
        self.selected_index = 0

    # display the menu on the terminal
    def display(
        self,
    ) -> None:
        """
        Display menu and get user selection.
        """
        
        self.selected_index = 0
        
        # Windows or Unix
        import platform
        system = platform.system()
        
        if system == "Windows":
            choice = self._get_choice_windows()
        else:
            choice = self._get_choice_unix()
            
        # Execute selected action
        self._execute_choice(choice)

    # render the menu with an arrow icon
    def _render_menu(
        self,
    ) -> str:
        """
        Render the menu with the arrow icon.
        """
        
        lines = ["? What would you like to do?"]
        for idx, (key, description) in enumerate(self.options):
            if idx == self.selected_index:
                lines.append(f"❯ {description}")
            else:
                lines.append(f"  {description}")
        return "\n".join(lines)

    # clear the screen
    def _clear_screen(
        self,
    ) -> None:
        """
        Clear the screen.
        """
        
        os.system("cls" if os.name == "nt" else "clear")

    # retrieve the choice from the user on Windows systems
    def _get_choice_windows(
        self,
    ) -> str:
        """
        Get user choice on Windows using msvcrt.
        """
        
        import msvcrt
        
        while True:
            self._clear_screen()
            print(self._render_menu())
            
            key = msvcrt.getch()
            
            if key == b'\r':  # Enter key
                return self.options[self.selected_index][0]
            elif key == b'\xe0':  # Special key (arrow keys)
                arrow = msvcrt.getch()
                if arrow == b'H':  # Up arrow
                    self.selected_index = (self.selected_index - 1) % len(self.options)
                elif arrow == b'P':  # Down arrow
                    self.selected_index = (self.selected_index + 1) % len(self.options)

    # retrieve the choice from the user on Unix systems
    def _get_choice_unix(
        self,
    ) -> str:
        """
        Get user choice on Unix systems.
        """
        
        import select
        import termios
        import tty
        
        def getch():
            fd = sys.stdin.fileno()
            old_settings = termios.tcgetattr(fd)
            try:
                tty.setraw(sys.stdin.fileno())
                ch = sys.stdin.read(1)
            finally:
                termios.tcsetattr(fd, termios.TCSADRAIN, old_settings)
            return ch
        
        while True:
            self._clear_screen()
            print(self._render_menu())
            
            if select.select([sys.stdin], [], [], 0)[0]:
                key = getch()
                
                if key == '\x1b':  # Escape sequence
                    getch()  # Skip '['
                    arrow = getch()
                    if arrow == 'A':  # Up arrow
                        self.selected_index = (self.selected_index - 1) % len(self.options)
                    elif arrow == 'B':  # Down arrow
                        self.selected_index = (self.selected_index + 1) % len(self.options)
                elif key == '\r' or key == '\n':  # Enter key
                    return self.options[self.selected_index][0]

    # run the choice the user have made
    def _execute_choice(
        self,
        choice: str,
    ) -> None:
        """
        Execute the selected choice.
        """
        
        self._clear_screen()
        
        if choice == "init":
            self._init_project()
        elif choice == "launch":
            self._launch_dashboard()

    # initialize the project with encryption key and salt key
    def _init_project(
        self,
    ) -> None:
        """
        Initialize project with encryption keys.
        """
        
        print("Initializing project.")
        
        encryption_key = EncryptionManager.generate_key()
        salt_key = EncryptionManager.generate_salt()
        salt_b64 = base64.b64encode(salt_key).decode("utf-8")

        env_path = os.path.join(os.getcwd(), ENV_FILE_NAME)
        
        if os.path.exists(env_path):
            print(f"'{ENV_FILE_NAME}' already exists.")
            response = input("Do you want to overwrite it? (y/n): ")
            if response.lower() != 'y':
                print("Initialization cancelled.")
                input("\nPress Enter to continue...")
                return

        content = (
            "ENCRYPTION_KEY="
            + encryption_key
            + "\n"
            + "SALT_KEY="
            + salt_b64
            + "\n"
        )
        with open(env_path, "w", encoding="utf-8") as f:
            f.write(content)

        # ensure .env.local is ignored by git
        gitignore_path = os.path.join(os.getcwd(), ".gitignore")
        gitignore_entry = ".env.local"
        if os.path.exists(gitignore_path):
            with open(gitignore_path, "r", encoding="utf-8") as f:
                gitignore_content = f.read()
            if gitignore_entry not in gitignore_content.splitlines():
                with open(gitignore_path, "a", encoding="utf-8") as f:
                    if not gitignore_content.endswith("\n"):
                        f.write("\n")
                    f.write(gitignore_entry + "\n")
        else:
            with open(gitignore_path, "w", encoding="utf-8") as f:
                f.write(gitignore_entry + "\n")

        print(f"✓ Created '{ENV_FILE_NAME}' with ENCRYPTION_KEY and SALT_KEY.")
        print("✓ Updated .gitignore with .env.local")
        print("\nYour project is now ready!")
        input("\nPress Enter to continue...")
    
    # launch the dashboard
    def _launch_dashboard(
        self,
    ) -> None:
        """
        Launch the dashboard.
        """
        
        print("Launching dashboard.")
        
        # Set environment variables for dashboard
        os.environ["SKYPYDB_PATH"] = self.path
        os.environ["SKYPYDB_PORT"] = str(self.dashboard_port)

        from ..dashboard.dashboard.dashboard import app

        try:
            import uvicorn
        except Exception as exc:
            print(f"Uvicorn is required to run the dashboard: {exc}", file=sys.stderr)
            input("\nPress Enter to continue...")
            sys.exit(1)

        # show the dashboard url
        print(f"Dashboard is running at http://127.0.0.1:'{self.dashboard_port}'")
        
        config = uvicorn.Config(
            app,
            host="127.0.0.1",
            port=self.dashboard_port,
            log_level="warning"
        )
        server = uvicorn.Server(config)
        self._dashboard_server = server
        server.run()

# dev argument
def cmd_dev(
    args: argparse.Namespace
) -> None:
    """
    Show interactive menu.
    """
    
    menu_options = [
        ("init", "Initialize project"),
        ("launch", "Launch dashboard"),
    ]
    
    menu = InteractiveMenu(menu_options)
    menu.display()

def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        prog="skypydb",
        description="skypydb Cli",
    )
    subparsers = parser.add_subparsers(dest="command", required=True)

    dev_parser = subparsers.add_parser(
        "dev",
        help="Interactive menu for skypydb",
    )
    dev_parser.set_defaults(func=cmd_dev)

    return parser

def main(
    argv: Optional[list[str]] = None
) -> None:
    parser = build_parser()
    args = parser.parse_args(argv)
    args.func(args)

if __name__ == "__main__":
    main()
