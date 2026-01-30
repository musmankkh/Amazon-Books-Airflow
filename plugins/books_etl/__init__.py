"""
Books ETL Package
"""

# --------------------
# Configuration
# --------------------
from plugins.books_etl.config import (
    POSTGRES_CONN_ID,
    CSV_DIR,
)

# --------------------
# Extractors
# --------------------
from plugins.books_etl.extractors import read_csv_files

# --------------------
# Transformers
# --------------------
from plugins.books_etl.transformers import transform_data

# --------------------
# Loaders
# --------------------
from plugins.books_etl.loaders import insert_data

__all__ = [
    "POSTGRES_CONN_ID",
    "CSV_DIR",
    "read_csv_files",
    "transform_data",
    "insert_data",
]

__version__ = "1.0.0"
