# src/snowpark_pipeline.py
import os
from typing import List
import snowflake.snowpark as snowpark
from snowflake.snowpark.functions import col

# ----------------------------
# Config with sane defaults
# Pull from env so secrets and URLs are not hard-coded in git
# ----------------------------
DB = os.getenv("SF_DB", "DATA_PIPELINE_DB")
SCHEMA = os.getenv("SF_SCHEMA", "CSV_FILES")
FILE_FORMAT_NAME = os.getenv("SF_FILE_FORMAT", "csv_format")
STAGE_NAME = os.getenv("SF_STAGE", "aws_stage")
TABLE_NAME = os.getenv("SF_TABLE", "ORDERS")
S3_URL = os.getenv("S3_URL", "s3://bucketsnowflakes3/")
COPY_PATTERN = os.getenv("COPY_PATTERN", ".*Order.*")

# Fully qualified object names
FQ_FILE_FORMAT = f"{DB}.{SCHEMA}.{FILE_FORMAT_NAME}"
FQ_STAGE = f"{DB}.{SCHEMA}.{STAGE_NAME}"
FQ_TABLE = f"{DB}.{SCHEMA}.{TABLE_NAME}"

def _exec(session: snowpark.Session, sql: str) -> None:
    """Small helper to run SQL and surface errors early."""
    session.sql(sql).collect()

def main(session: snowpark.Session) -> List[snowpark.Row]:
    """
    Creates database, schema, file format, stage, and table in Snowflake,
    loads data from S3, performs simple transforms, and returns first 10 rows.

    Returns:
        List[Row]: first 10 transformed rows
    """
    # 1. Create DB and schema (idempotent)
    _exec(session, f"CREATE DATABASE IF NOT EXISTS {DB}")
    _exec(session, f"CREATE SCHEMA IF NOT EXISTS {DB}.{SCHEMA}")

    # 2. Create file format
    _exec(
        session,
        f"""
        CREATE FILE FORMAT IF NOT EXISTS {FQ_FILE_FORMAT}
        TYPE = CSV
        FIELD_DELIMITER = ','
        SKIP_HEADER = 1
        EMPTY_FIELD_AS_NULL = TRUE
        """
    )

    # 3. Create external stage pointing to S3 with the file format
    _exec(
        session,
        f"""
        CREATE STAGE IF NOT EXISTS {FQ_STAGE}
        URL = '{S3_URL}'
        FILE_FORMAT = (FORMAT_NAME = {FQ_FILE_FORMAT})
        """
    )

    # 4. Create target table
    _exec(
        session,
        f"""
        CREATE TABLE IF NOT EXISTS {FQ_TABLE}(
            ID STRING,
            Amount NUMBER,
            Profit NUMBER,
            Quantity NUMBER,
            Category STRING,
            Sub_Category STRING
        )
        """
    )

    # 5. Load data
    # ON_ERROR=CONTINUE prevents the load from aborting due to a few bad rows during early testing
    _exec(
        session,
        f"""
        COPY INTO {FQ_TABLE}
        FROM @{FQ_STAGE}
        FILE_FORMAT = (FORMAT_NAME = {FQ_FILE_FORMAT})
        PATTERN = '{COPY_PATTERN}'
        ON_ERROR = CONTINUE
        """
    )

    # 6. Query and transform using Snowpark
    df = session.table(FQ_TABLE)

    # 7. Example transforms: cast to numeric
    df_transformed = (
        df.with_column("Amount_num", col("Amount").cast("DOUBLE"))
          .with_column("Profit_num", col("Profit").cast("DOUBLE"))
    )

    # 8. Return first 10 rows as a Python list, not a DataFrame
    return df_transformed.limit(10).collect()

