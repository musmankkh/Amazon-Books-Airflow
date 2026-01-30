import pandas as pd
import boto3
import logging
from pathlib import Path
from plugins.books_etl.config import TMP_DIR

logger = logging.getLogger(__name__)

S3_BUCKET = "books-etl-data"
S3_PREFIX = "raw/"   # where Books*.csv live


def read_csv_files(file_pattern="Books", **context) -> str:
    s3 = boto3.client("s3")

    response = s3.list_objects_v2(
        Bucket=S3_BUCKET,
        Prefix=S3_PREFIX
    )

    csv_keys = [
        obj["Key"]
        for obj in response.get("Contents", [])
        if obj["Key"].endswith(".csv") and file_pattern in obj["Key"]
    ]

    if not csv_keys:
        raise FileNotFoundError("No CSV files found in S3")

    dfs = []
    for key in csv_keys:
        logger.info(f"Reading s3://{S3_BUCKET}/{key}")
        obj = s3.get_object(Bucket=S3_BUCKET, Key=key)
        dfs.append(pd.read_csv(obj["Body"]))

    df = pd.concat(dfs, ignore_index=True)

    output = TMP_DIR / "raw_books.json"
    df.to_json(output, orient="records")

    logger.info(f"Extracted {len(df)} rows → {output}")
    return str(output)
