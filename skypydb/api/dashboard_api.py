"""
API endpoints for the dashboard.
"""

import os
from typing import Dict, List
from ..db.database import Database


# retrieve database instance from environment
def get_database() -> Database:
    """
    Get database instance from environment.
    """

    path = os.environ.get('SKYPYDB_PATH', './skypydb/skypy.db')
    return Database(path)


# retrieve all tables
def get_all_tables() -> List[str]:
    """
    Get all table names.
    """

    db = get_database()

    try:
        return db.get_all_tables()
    finally:
        db.close()


# retrieve specific data from a table
def get_table_data(
    table_name: str,
) -> List[Dict]:
    """
    Get all data from a table.
    """

    db = get_database()

    try:
        return db.get_all_data(table_name)
    finally:
        db.close()


# retrieve a table with their column names
def get_table_schema(
    table_name: str,
) -> List[str]:
    """
    Retrieve the column names for the specified table.

    Returns:
        columns (List[str]): Column names for the specified table.
    """

    db = get_database()

    try:
        return db.get_table_columns(table_name)
    finally:
        db.close()
