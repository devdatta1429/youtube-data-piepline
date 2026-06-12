
import json
import os
import logging
from datetime import datetime, timezone
from urllib.parse import unquote_plus
from io import BytesIO

import boto3
import pandas as pd

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


def validate_category_data(df):

    if df.empty:
        raise ValueError(
            "No category records found."
        )

    before = len(df)

    if "id" in df.columns:
        df = df.drop_duplicates(
            subset=["id"],
            keep="last"
        )

    after = len(df)

    logger.info(
        f"Removed {before - after} duplicates"
    )

    return df


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

            df["_ingestion_timestamp"] = (
                datetime.now(
                    timezone.utc
                ).isoformat()
            )

            df["_source_file"] = key

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

            df["region"] = region

            logger.info(
                f"Region: {region}"
            )

            # -------------------------------------
            # Convert To Parquet
            # -------------------------------------

            parquet_buffer = BytesIO()

            df.to_parquet(
                parquet_buffer,
                engine="pyarrow",
                index=False
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

