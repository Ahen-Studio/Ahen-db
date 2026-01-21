"""
API endpoints for the dashboard.
"""

import os
from typing import Dict, List

from ..db.database import Database


def get_database() -> Database:
    """
    Get database instance from environment.
    """

    path = os.environ.get('SKYPYDB_PATH', './data/skypy.db')
    return Database(path)


def get_all_tables() -> List[str]:
    """
    Get all table names.
    """

    db = get_database()

    try:
        return db.get_all_tables()
    finally:
        db.close()


def get_table_data(table_name: str) -> List[Dict]:
    """
    Get all data from a table.
    """

    db = get_database()

    try:
        return db.get_all_data(table_name)
    finally:
        db.close()


def get_table_schema(table_name: str) -> List[str]:
    """
    Get column names for a table.
    """

    db = get_database()

    try:
        return db.get_table_columns(table_name)
    finally:
        db.close()
