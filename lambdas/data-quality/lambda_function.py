
import os
import json
import time
import logging
from datetime import datetime, timezone, timedelta

import boto3
import pandas as pd

logger = logging.getLogger()
logger.setLevel(logging.INFO)

# ------------------------------------------------------------------
# AWS Clients
# ------------------------------------------------------------------

athena = boto3.client("athena")
s3 = boto3.client("s3")
sns = boto3.client("sns")

# ------------------------------------------------------------------
# Environment Variables
# ------------------------------------------------------------------

ATHENA_RESULTS_BUCKET = os.environ["ATHENA_RESULTS_BUCKET"]

SNS_TOPIC = os.environ.get(
    "SNS_ALERT_TOPIC_ARN",
    ""
)

# ------------------------------------------------------------------
# Thresholds
# ------------------------------------------------------------------

MIN_ROW_COUNT = 10
MAX_NULL_PCT = 5.0
MAX_VIEWS = 50_000_000_000
FRESHNESS_HOURS = 48

CRITICAL_COLUMNS = {
    "clean_statistics": [
        "video_id",
        "title",
        "channel_title",
        "views",
        "region"
    ],
    "clean_reference_data": [
        "category_id",
        "category_title",
        "region"
    ]
}


# ------------------------------------------------------------------
# Athena Query
# ------------------------------------------------------------------

def run_athena_query(sql, database):

    response = athena.start_query_execution(
        QueryString=sql,
        QueryExecutionContext={
            "Database": database
        },
        ResultConfiguration={
            "OutputLocation":
            f"s3://{ATHENA_RESULTS_BUCKET}/"
        }
    )

    query_id = response["QueryExecutionId"]

    while True:

        status = athena.get_query_execution(
            QueryExecutionId=query_id
        )["QueryExecution"]["Status"]["State"]

        if status == "SUCCEEDED":
            break

        if status in ["FAILED", "CANCELLED"]:
            raise Exception(
                f"Athena query failed: {status}"
            )

        time.sleep(2)

    result_location = athena.get_query_execution(
        QueryExecutionId=query_id
    )["QueryExecution"]["ResultConfiguration"]["OutputLocation"]

    bucket = result_location.replace(
        "s3://",
        ""
    ).split("/")[0]

    key = "/".join(
        result_location.replace(
            "s3://",
            ""
        ).split("/")[1:]
    )

    obj = s3.get_object(
        Bucket=bucket,
        Key=key
    )

    return pd.read_csv(
        obj["Body"]
    )


# ------------------------------------------------------------------
# DQ Checks
# ------------------------------------------------------------------

def check_row_count(df, table):

    count = len(df)

    return {
        "check": "row_count",
        "table": table,
        "passed": count >= MIN_ROW_COUNT,
        "value": count
    }


def check_schema(df, table):

    expected = set(
        CRITICAL_COLUMNS.get(
            table,
            []
        )
    )

    actual = set(df.columns)

    missing = list(
        expected - actual
    )

    return {
        "check": "schema",
        "table": table,
        "passed": len(missing) == 0,
        "missing": missing
    }


def check_nulls(df, table):

    results = []

    for col in CRITICAL_COLUMNS.get(
        table,
        []
    ):

        if col not in df.columns:

            results.append({
                "check": "nulls",
                "table": table,
                "column": col,
                "passed": False
            })

            continue

        pct = (
            df[col]
            .isna()
            .mean()
            * 100
        )

        results.append({
            "check": "nulls",
            "table": table,
            "column": col,
            "passed": pct <= MAX_NULL_PCT,
            "value": round(pct, 2)
        })

    return results


def check_views(df):

    if "views" not in df.columns:
        return []

    negative = (
        df["views"] < 0
    ).sum()

    extreme = (
        df["views"] > MAX_VIEWS
    ).sum()

    return [{
        "check": "view_range",
        "table": "clean_statistics",
        "passed": (
            negative == 0
            and extreme == 0
        ),
        "negative": int(negative),
        "extreme": int(extreme)
    }]


# ------------------------------------------------------------------
# Alert
# ------------------------------------------------------------------

def send_alert(results):

    if not SNS_TOPIC:
        return

    sns.publish(
        TopicArn=SNS_TOPIC,
        Subject="YT Pipeline DQ Failed",
        Message=json.dumps(
            results,
            indent=2,
            default=str
        )
    )


# ------------------------------------------------------------------
# Lambda Handler
# ------------------------------------------------------------------

def lambda_handler(event, context):

    database = event.get(
        "database",
        "yt_pipeline_silver_dev"
    )

    tables = event.get(
        "tables",
        [
            "clean_statistics",
            "clean_reference_data"
        ]
    )

    all_results = []
    overall_passed = True

    for table in tables:

        logger.info(
            f"Checking {table}"
        )

        try:

            query = (
                f'SELECT * '
                f'FROM "{table}" '
                f'LIMIT 10000'
            )

            df = run_athena_query(
                query,
                database
            )

        except Exception as e:

            overall_passed = False

            all_results.append({
                "table": table,
                "passed": False,
                "error": str(e)
            })

            continue

        checks = []

        checks.append(
            check_row_count(
                df,
                table
            )
        )

        checks.append(
            check_schema(
                df,
                table
            )
        )

        checks.extend(
            check_nulls(
                df,
                table
            )
        )

        if table == "clean_statistics":

            checks.extend(
                check_views(df)
            )

        for c in checks:

            if not c["passed"]:
                overall_passed = False

        all_results.extend(
            checks
        )

    if not overall_passed:

        send_alert(
            all_results
        )

    return {
        "quality_passed":
        overall_passed,
        "results":
        all_results
    }

