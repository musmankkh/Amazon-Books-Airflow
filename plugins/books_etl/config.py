from pathlib import Path

POSTGRES_CONN_ID = "book_connection"

BASE_DIR = Path("/opt/airflow")
CSV_DIR = BASE_DIR / "data"
TMP_DIR = BASE_DIR / "data" / "tmp"
TMP_DIR.mkdir(exist_ok=True)

BATCH_SIZE = 1000
DUPLICATE_CHECK_COLUMNS = ["title"]
