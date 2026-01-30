from pathlib import Path

POSTGRES_CONN_ID = "book_connection"

TMP_DIR = Path("/tmp/books_etl")
TMP_DIR.mkdir(exist_ok=True)

BATCH_SIZE = 1000
DUPLICATE_CHECK_COLUMNS = ["title"]
