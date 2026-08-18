
import json
import os
import logging
from datetime import datetime, timezone
from urllib.parse import unquote_plus
from io import BytesIO

import boto3
import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq

# ─────────────────────────────────────────────────────────────
# Logging
# ─────────────────────────────────────────────────────────────

logger = logging.getLogger()
logger.setLevel(logging.INFO)

# ─────────────────────────────────────────────────────────────
# Configuration
# ─────────────────────────────────────────────────────────────

SILVER_BUCKET = os.environ["S3_BUCKET_SILVER"].strip()

SNS_TOPIC = os.environ.get(
    "SNS_ALERT_TOPIC_ARN",
    ""
).strip()

s3_client = boto3.client("s3")
sns_client = boto3.client("sns")

# ─────────────────────────────────────────────────────────────
# Helpers
# ─────────────────────────────────────────────────────────────

def read_json_from_s3(bucket, key):

    response = s3_client.get_object(
        Bucket=bucket,
        Key=key
    )

    content = (
        response["Body"]
        .read()
        .decode("utf-8")
    )

    return json.loads(content)


def normalize_category_columns(df):
    """
    Map whatever the raw JSON's field names happen to be onto the
    fixed Silver schema (category_id, category_name). Without this,
    a YouTube Data API response (id / snippet.title) and a Kaggle-style
    file (category_id / category_name) produce differently-named
    columns, and Athena silently returns NULL for any column the
    Glue table declares that a given Parquet file doesn't have.
    """

    rename_map = {}

    if "id" in df.columns and "category_id" not in df.columns:
        rename_map["id"] = "category_id"

    if "snippet.title" in df.columns and "category_name" not in df.columns:
        rename_map["snippet.title"] = "category_name"

    if rename_map:
        df = df.rename(columns=rename_map)

    missing = [
        col for col in ("category_id", "category_name")
        if col not in df.columns
    ]

    if missing:
        raise ValueError(
            f"Source JSON is missing expected fields after normalization: {missing}. "
            f"Columns present: {list(df.columns)}"
        )

    # The Glue table declares category_id as bigint, but the raw JSON's
    # "id" field comes through as a string (e.g. "1", "17"). Without an
    # explicit cast, Parquet writes it as BINARY and Athena throws
    # HIVE_BAD_DATA on read. Coerce to int64 here so the Parquet physical
    # type always matches the table schema.
    df["category_id"] = pd.to_numeric(
        df["category_id"],
        errors="coerce"
    )

    bad_ids = df["category_id"].isna().sum()

    if bad_ids:
        logger.warning(
            f"Dropping {bad_ids} rows with non-numeric category_id"
        )
        df = df.dropna(subset=["category_id"])

    df["category_id"] = df["category_id"].astype("int64")

    return df


def validate_category_data(df):

    if df.empty:
        raise ValueError(
            "No category records found."
        )

    df = normalize_category_columns(df)

    before = len(df)

    df = df.drop_duplicates(
        subset=["category_id"],
        keep="last"
    )

    after = len(df)

    logger.info(
        f"Removed {before - after} duplicates"
    )

    # Keep only the columns the Silver schema expects, plus metadata,
    # so a source file with extra fields (kind, etag, assignable,
    # channelId, ...) can't drift the table schema again.
    keep_cols = [
        c for c in ("category_id", "category_name")
        if c in df.columns
    ]

    return df[keep_cols].copy()


def send_alert(subject, message):

    if not SNS_TOPIC:
        logger.info(
            "SNS topic not configured."
        )
        return

    try:

        sns_client.publish(
            TopicArn=SNS_TOPIC,
            Subject=subject[:100],
            Message=message
        )

    except Exception as e:

        logger.error(
            f"SNS Error: {e}"
        )


# ─────────────────────────────────────────────────────────────
# Lambda Handler
# ─────────────────────────────────────────────────────────────

def lambda_handler(event, context):

    records = event.get(
        "Records",
        []
    )

    processed = []
    errors = []

    for record in records:

        key = "unknown"

        try:

            bucket = (
                record["s3"]
                ["bucket"]
                ["name"]
            )

            key = unquote_plus(
                record["s3"]
                ["object"]
                ["key"]
            )

            logger.info(
                f"Processing: s3://{bucket}/{key}"
            )

            # -------------------------------------
            # Read JSON
            # -------------------------------------

            raw_data = read_json_from_s3(
                bucket,
                key
            )

            # -------------------------------------
            # Flatten JSON
            # -------------------------------------

            if (
                "items" in raw_data
                and isinstance(
                    raw_data["items"],
                    list
                )
            ):

                df = pd.json_normalize(
                    raw_data["items"]
                )

            else:

                df = pd.json_normalize(
                    raw_data
                )

            logger.info(
                f"Raw Shape: {df.shape}"
            )

            # -------------------------------------
            # Validation
            # -------------------------------------

            df = validate_category_data(df)

            # -------------------------------------
            # Metadata
            # -------------------------------------

            df["_processed_at"] = (
                pd.Timestamp.now("UTC")
                .tz_localize(None)
                .floor("ms")
            )

            # -------------------------------------
            # Extract Region
            # -------------------------------------

            region = "unknown"

            for part in key.split("/"):

                if part.startswith(
                    "region="
                ):

                    region = (
                        part
                        .split("=")[1]
                        .lower()
                    )

                    break

            logger.info(
                f"Region: {region}"
            )

            # -------------------------------------
            # Convert To Parquet (explicit schema —
            # do not rely on pandas dtype inference,
            # which has proven inconsistent across
            # pandas/pyarrow versions in this runtime)
            # -------------------------------------

            REFERENCE_SCHEMA = pa.schema([
                pa.field("category_id", pa.int64()),
                pa.field("category_name", pa.string()),
                pa.field("_processed_at", pa.timestamp("ms")),
            ])

            arrow_table = pa.Table.from_pandas(
                df,
                schema=REFERENCE_SCHEMA,
                preserve_index=False
            )

            parquet_buffer = BytesIO()

            pq.write_table(
                arrow_table,
                parquet_buffer
            )

            output_key = (
                f"youtube/reference_data/"
                f"region={region}/"
                f"reference_data.parquet"
            )

            s3_client.put_object(
                Bucket=SILVER_BUCKET,
                Key=output_key,
                Body=parquet_buffer.getvalue()
            )

            logger.info(
                f"Written: "
                f"s3://{SILVER_BUCKET}/{output_key}"
            )

            processed.append(
                {
                    "key": key,
                    "region": region,
                    "rows": len(df)
                }
            )

        except Exception as e:

            logger.error(
                f"Error processing {key}: {e}",
                exc_info=True
            )

            errors.append(
                {
                    "key": key,
                    "error": str(e)
                }
            )

    # -----------------------------------------
    # SNS Alert
    # -----------------------------------------

    if errors:

        send_alert(
            "[YT Pipeline] Reference Transform Failed",
            json.dumps(
                errors,
                indent=2
            )
        )

    return {
        "statusCode": 200,
        "processed": processed,
        "errors": errors
    }

