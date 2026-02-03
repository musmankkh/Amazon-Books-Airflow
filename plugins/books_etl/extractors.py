import pandas as pd
from pathlib import Path
import logging
from plugins.books_etl.config import CSV_DIR, TMP_DIR

logger = logging.getLogger(__name__)

def read_csv_files(file_pattern="Books*.csv", **context) -> str:
    csv_files = list(Path(CSV_DIR).glob(file_pattern))
    if not csv_files:
        raise FileNotFoundError("No CSV files found")

    df = pd.concat([pd.read_csv(f) for f in csv_files], ignore_index=True)

    output = TMP_DIR / "raw_books.json"
    df.to_json(output, orient="records")

    logger.info(f"Extracted {len(df)} rows → {output}")
    return str(output)
