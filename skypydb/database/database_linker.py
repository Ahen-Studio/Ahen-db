"""
Database link metadata utilities for dashboard auto-discovery.
"""

import struct
from pathlib import Path
from typing import (
    Dict,
    List,
    Optional,
    Tuple
)

class DatabaseLinker:
    """
    Manages database link metadata for automatic discovery and classification.

    The DatabaseLinker class handles the creation, reading, and discovery of binary
    metadata files that store information about database paths and their types.
    This enables dashboard applications to automatically discover and categorize
    databases without requiring explicit configuration.
    """

    def __init__(
        self,
        folder: str = "link",
        binary_file_type: Dict[str, str] = {"reactive": "reactivetype.bin", "vector": "vectortype.bin"},
        magic: bytes = b"SKYPYLINKER"
    ):
        """
        Initialize the DatabaseLinker with custom configuration.

        Args:
            folder: Name of the sidecar folder to store metadata files. This folder is created adjacent to database files. Defaults to "link".
            binary_file_type: Mapping from database type to metadata filename. Defaults to {"reactive": "reactivetype.bin", "vector": "vectortype.bin"}.
            magic: Magic bytes for file validation and identification. Must be 11 bytes. Defaults to b"SKYPYLINKER". Changing this affects discovery behavior.
        """

        self.folder = folder
        self.binary_file_type = binary_file_type
        self.magic = magic

    def discover_database_links(
        self,
        root: Optional[Path] = None
    ) -> List[Dict[str, str]]:
        """
        Recursively discover all database link metadata files from a root folder.

        This method searches for binary metadata files matching the pattern
        **/SKYPYLINKER/*.bin and decodes them to extract database information.

        Args:
            root: Root directory to start searching from. Defaults to the current working directory. If None, uses Path.cwd().

        Returns:
            List: List of dictionaries, each containing:
            type: Database type ("reactive" or "vector")
            path: Full path to the database file
            metadata_file: Full path to the metadata binary file
        """

        search_root = root or Path.cwd()
        discovered: List[Dict[str, str]] = []
        pattern = f"**/{self.magic}/*.bin"
        for metadata_file in search_root.glob(pattern):
            if not metadata_file.is_file():
                continue
            parsed_entries = self._read_link_metadata(metadata_file)
            discovered.extend(parsed_entries)
        return discovered

    def ensure_db_link_metadata(
        self,
        path: str,
        db_type: Optional[str] = None
    ) -> Dict[str, str]:
        """
        Create or update a sidecar link folder and binary metadata file for a database.

        This method creates the metadata directory structure and writes a binary
        metadata file containing the database path and type. If metadata already
        exists for the given database, the path is preserved and deduplicated.

        Args:
            path: Path to the database file. Can be absolute or relative.
            db_type: Explicit database type ("reactive" or "vector"). If not provided, the type will be auto-detected by inspecting the database schema.

        Returns:
            Dict[str, str]: Dictionary containing:
            type: The database type used
            path: The normalized absolute path to the database
        """

        resolved_db_path = self._resolve_db_path(path)
        resolved_type = db_type
        if resolved_type is None:
            raise ValueError(f"db_type must be specified ('reactive' or 'vector')")
        metadata_path = self._metadata_file_for_db(resolved_db_path, resolved_type)
        metadata_path.parent.mkdir(parents=True, exist_ok=True)

        existing_paths: List[str] = []
        if metadata_path.exists():
            try:
                raw = metadata_path.read_bytes()
                decoded = self._decode_binary_payload(raw)
                if decoded is not None and decoded[0] == resolved_type:
                    existing_paths = decoded[1]
            except OSError:
                existing_paths = []

        normalized = str(resolved_db_path)
        merged_paths = list(dict.fromkeys(existing_paths + [normalized]))

        payload = {
            "type": resolved_type,
            "path": normalized
        }
        metadata_path.write_bytes(
            self._encode_binary_payload(
                resolved_type,
                merged_paths
            )
        )
        return payload

    def _resolve_db_path(
        self,
        path: str
    ) -> Path:
        """
        Resolve a database path to an absolute Path object.

        Converts relative paths to absolute paths by joining with the current
        working directory, then resolves any symbolic links or path components.

        Args:
            path: Database path as a string, either absolute or relative.

        Returns:
            Path: Resolved absolute Path object pointing to the database file.
        """

        db_path = Path(path)
        return db_path if db_path.is_absolute() else (Path.cwd() / db_path).resolve()

    def _metadata_file_for_db(
        self,
        db_path: Path,
        db_type: str
    ) -> Path:
        """
        Get the path to the metadata file for a database.

        Constructs the metadata file path by creating a link folder in the
        same directory as the database and appending the appropriate binary
        filename based on the database type.

        Args:
            db_path: Path to the database file.
            db_type: Database type ("reactive" or "vector").

        Returns:
            Path: Full path to the metadata binary file.
        """

        link_dir = db_path.parent / self.folder
        return link_dir / self.binary_file_type[db_type]

    def _encode_binary_payload(
        self,
        db_type: str,
        db_paths: List[str]
    ) -> bytes:
        """
        Encode database metadata into a binary payload.

        Args:
            db_type: Database type ("reactive" or "vector").
            db_paths: List of database file paths to encode.

        Returns:
            bytes: Complete binary payload suitable for writing to a file.

        Raises:
            ValueError: If db_type is not "reactive" or "vector".
        """

        type_code = 1 if db_type == "reactive" else 2
        payload = bytearray()
        payload.extend(self.magic)
        payload.extend(struct.pack(">BI", type_code, len(db_paths)))
        for db_path in db_paths:
            path_bytes = db_path.encode("utf-8")
            payload.extend(struct.pack(">I", len(path_bytes)))
            payload.extend(path_bytes)
        return bytes(payload)

    def _decode_binary_payload(
        self,
        raw: bytes
    ) -> Optional[Tuple[str, List[str]]]:
        """
        Decode a binary payload into database metadata.

        Parses binary data created by _encode_binary_payload and extracts
        the database type and associated paths.

        Args:
            raw: Raw binary data from a metadata file.

        Returns:
            Optional[Tuple[str, List[str]]]: Tuple of (db_type, db_paths) if decoding is successful, None if the data is invalid or corrupted.
            db_type: "reactive" or "vector"
            db_paths: List of database file paths

        Validation:
            Checks magic header matches
            Verifies type code is valid (1 or 2)
            Ensures all path data fits within the payload
            Validates UTF-8 encoding for all paths
        """

        if len(raw) < len(self.magic) + 5 or not raw.startswith(self.magic):
            return None

        offset = len(self.magic)
        type_code, count = struct.unpack(">BI", raw[offset:offset + 5])
        db_type = "reactive" if type_code == 1 else "vector" if type_code == 2 else None
        if db_type is None:
            return None

        paths: List[str] = []
        cursor = offset + 5
        for _ in range(count):
            if cursor + 4 > len(raw):
                return None
            path_len = struct.unpack(">I", raw[cursor:cursor + 4])[0]
            cursor += 4
            if cursor + path_len > len(raw):
                return None
            try:
                db_path = raw[cursor:cursor + path_len].decode("utf-8")
            except UnicodeDecodeError:
                return None
            if db_path:
                paths.append(db_path)
            cursor += path_len
        if cursor != len(raw):
            return None
        return db_type, paths

    def _read_link_metadata(
        self,
        path: Path
    ) -> List[Dict[str, str]]:
        """
        Read and parse a database link metadata file.

        Reads a binary metadata file, decodes its contents, and returns a list
        of database entries with their metadata file path.

        Args:
            path: Path to the metadata binary file.

        Returns:
            List[Dict[str, str]]: List of database entries, each containing:
            type: Database type ("reactive" or "vector")
            path: Database file path
            metadata_file: Path to this metadata file
        """

        try:
            raw = path.read_bytes()
        except OSError:
            return []

        decoded = self._decode_binary_payload(raw)
        if decoded is None:
            return []

        db_type, db_paths = decoded
        entries: List[Dict[str, str]] = []
        for db_path in db_paths:
            entries.append(
                {
                    "type": db_type,
                    "path": db_path,
                    "metadata_file": str(path)
                }
            )
        return entries