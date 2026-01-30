import pandas as pd
import hashlib
import logging
from plugins.books_etl.config import TMP_DIR, DUPLICATE_CHECK_COLUMNS

logger = logging.getLogger(__name__)


def transform_data(**context) -> str:
    ti = context["ti"]
    input_path = ti.xcom_pull(task_ids="read_csv")

    if not input_path:
        raise ValueError("No input file path received from read_csv")

    df = pd.read_json(input_path)

    logger.info(f"Loaded {len(df)} raw records")

    # ----------------------------------------------------
    # Required column validation (structure-level)
    # ----------------------------------------------------
    required_columns = {"Title", "Author"}
    if not required_columns.issubset(df.columns):
        missing = required_columns - set(df.columns)
        raise ValueError(f"Missing required columns: {missing}")

    # ----------------------------------------------------
    # Rename columns to DB schema
    # ----------------------------------------------------
    df = df.rename(columns={
        "Title": "title",
        "Author": "author",
        "Main Genre": "category",
        "Price": "price",
        "Rating": "rating",
    })

    # ----------------------------------------------------
    # Clean string columns
    # ----------------------------------------------------
    df["title"] = df["title"].astype(str).str.strip()
    df["author"] = df["author"].astype(str).str.strip()

    # Convert empty / invalid strings to NULL
    df.replace(
        {"": None, "nan": None, "None": None},
        inplace=True
    )

    # ----------------------------------------------------
    # Drop rows with NULL title or author (FIX)
    # ----------------------------------------------------
    before = len(df)
    df = df.dropna(subset=["title", "author"])
    after = len(df)

    dropped = before - after
    if dropped > 0:
        logger.warning(f"Dropped {dropped} rows with NULL title/author")

    # ----------------------------------------------------
    # Clean price
    # ----------------------------------------------------
    if "price" in df.columns:
        df["price"] = (
            df["price"]
            .astype(str)
            .str.replace("₹", "", regex=False)
            .str.replace(",", "", regex=False)
            .str.strip()
        )
        df["price"] = pd.to_numeric(df["price"], errors="coerce").fillna(0)

    # ----------------------------------------------------
    # Generate record hash (deduplication)
    # ----------------------------------------------------
    def generate_hash(row):
        values = [
            str(row.get(col, "")).lower().strip()
            for col in DUPLICATE_CHECK_COLUMNS
        ]
        return hashlib.md5("|".join(values).encode()).hexdigest()

    df["record_hash"] = df.apply(generate_hash, axis=1)

    # ----------------------------------------------------
    # Deduplicate
    # ----------------------------------------------------
    before_dedup = len(df)
    df = df.drop_duplicates(subset=["record_hash"])
    after_dedup = len(df)

    if before_dedup != after_dedup:
        logger.info(
            f"Deduplicated {before_dedup - after_dedup} records"
        )

    # ----------------------------------------------------
    # Write cleaned data to temp file
    # ----------------------------------------------------
    output_path = TMP_DIR / "clean_books.json"
    df.to_json(output_path, orient="records")

    logger.info(f"Transformation complete: {len(df)} clean records")
    logger.info(f"Output written to {output_path}")

    return str(output_path)
