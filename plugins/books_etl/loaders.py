import pandas as pd
import logging
from airflow.providers.postgres.hooks.postgres import PostgresHook
from plugins.books_etl.config import POSTGRES_CONN_ID, BATCH_SIZE

logger = logging.getLogger(__name__)

# Explicit column order (MUST match SQL)
INSERT_COLUMNS = [
    "title",
    "author",
    "price",
    "category",
    "rating",
    "record_hash",
]


def insert_data(**context):
    ti = context["ti"]
    path = ti.xcom_pull(task_ids="transform_data")

    if not path:
        raise ValueError("No transformed data path received")

    df = pd.read_json(path)

    # 🔒 Ensure only required columns and correct order
    missing = set(INSERT_COLUMNS) - set(df.columns)
    if missing:
        raise ValueError(f"Missing required columns for insert: {missing}")

    df = df[INSERT_COLUMNS]

    hook = PostgresHook(postgres_conn_id=POSTGRES_CONN_ID)
    conn = hook.get_conn()
    cur = conn.cursor()

    sql = """
        INSERT INTO books (
            title,
            author,
            price,
            category,
            rating,
            record_hash
        )
        VALUES (%s, %s, %s, %s, %s, %s)
        ON CONFLICT (record_hash) DO NOTHING
    """

    total_inserted = 0

    try:
        for i in range(0, len(df), BATCH_SIZE):
            batch = df.iloc[i : i + BATCH_SIZE]

            # Convert rows to tuples (IMPORTANT)
            values = [tuple(row) for row in batch.to_numpy()]

            cur.executemany(sql, values)
            total_inserted += cur.rowcount

        conn.commit()
        logger.info(f"Inserted {total_inserted} records")

    except Exception as e:
        conn.rollback()
        logger.error("Insert failed", exc_info=True)
        raise

    finally:
        cur.close()
        conn.close()
