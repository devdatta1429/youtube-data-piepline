# import os
# import json
# import time
# import logging

# import boto3
# import pandas as pd


# logger = logging.getLogger()
# logger.setLevel(logging.INFO)


# # ------------------------------------------------------------------
# # AWS Clients
# # ------------------------------------------------------------------

# athena = boto3.client("athena")
# s3 = boto3.client("s3")
# sns = boto3.client("sns")


# # ------------------------------------------------------------------
# # Environment Variables
# # ------------------------------------------------------------------

# ATHENA_RESULTS_BUCKET = os.environ["ATHENA_RESULTS_BUCKET"]

# SNS_ALERT_TOPIC_ARN = os.environ.get(
#     "SNS_ALERT_TOPIC_ARN",
#     ""
# )


# # ------------------------------------------------------------------
# # Thresholds
# # ------------------------------------------------------------------

# MIN_ROW_COUNT = 10
# MAX_NULL_PCT = 5.0
# MAX_VIEWS = 50_000_000_000


# # ------------------------------------------------------------------
# # Critical Columns
# # ------------------------------------------------------------------
# #
# # These MUST match the actual Silver schemas.
# #
# # clean_statistics:
# #   video_id
# #   title
# #   channel_title
# #   views
# #   region
# #
# # clean_reference_data:
# #   category_id
# #   category_name
# #   region
# #
# # ------------------------------------------------------------------

# CRITICAL_COLUMNS = {
#     "clean_statistics": [
#         "video_id",
#         "title",
#         "channel_title",
#         "views",
#         "region"
#     ],
#     "clean_reference_data": [
#         "category_id",
#         "category_name",
#         "region"
#     ]
# }


# # ------------------------------------------------------------------
# # Athena Query
# # ------------------------------------------------------------------

# def run_athena_query(sql, database):

#     response = athena.start_query_execution(
#         QueryString=sql,

#         QueryExecutionContext={
#             "Database": database
#         },

#         ResultConfiguration={
#             "OutputLocation":
#                 f"s3://{ATHENA_RESULTS_BUCKET}/"
#         }
#     )

#     query_id = response["QueryExecutionId"]

#     logger.info(
#         f"Athena Query ID: {query_id}"
#     )

#     while True:

#         response = athena.get_query_execution(
#             QueryExecutionId=query_id
#         )

#         status = (
#             response["QueryExecution"]
#             ["Status"]
#             ["State"]
#         )

#         logger.info(
#             f"Athena query status: {status}"
#         )

#         if status == "SUCCEEDED":
#             break

#         if status in ["FAILED", "CANCELLED"]:

#             reason = (
#                 response["QueryExecution"]
#                 ["Status"]
#                 .get("StateChangeReason", "Unknown")
#             )

#             raise Exception(
#                 f"Athena query failed: "
#                 f"{status} - {reason}"
#             )

#         time.sleep(2)

#     result_location = (
#         response["QueryExecution"]
#         ["ResultConfiguration"]
#         ["OutputLocation"]
#     )

#     bucket = (
#         result_location
#         .replace("s3://", "")
#         .split("/")[0]
#     )

#     key = "/".join(
#         result_location
#         .replace("s3://", "")
#         .split("/")[1:]
#     )

#     obj = s3.get_object(
#         Bucket=bucket,
#         Key=key
#     )

#     return pd.read_csv(
#         obj["Body"]
#     )


# # ------------------------------------------------------------------
# # Row Count Check
# # ------------------------------------------------------------------

# def check_row_count(df, table):

#     count = len(df)

#     return {
#         "check": "row_count",
#         "table": table,
#         "passed": count >= MIN_ROW_COUNT,
#         "value": count
#     }


# # ------------------------------------------------------------------
# # Schema Check
# # ------------------------------------------------------------------

# def check_schema(df, table):

#     expected = set(
#         CRITICAL_COLUMNS.get(
#             table,
#             []
#         )
#     )

#     actual = set(df.columns)

#     missing = list(
#         expected - actual
#     )

#     return {
#         "check": "schema",
#         "table": table,
#         "passed": bool(len(missing) == 0),
#         "missing": missing
#     }

# # ------------------------------------------------------------------
# # Null Check
# # ------------------------------------------------------------------

# def check_nulls(df, table):

#     results = []

#     for col in CRITICAL_COLUMNS.get(table, []):

#         if col not in df.columns:

#             results.append({
#                 "check": "nulls",
#                 "table": table,
#                 "column": col,
#                 "passed": False
#             })

#             continue

#         pct = (
#             df[col]
#             .isna()
#             .mean()
#             * 100
#         )

#         results.append({
#             "check": "nulls",
#             "table": table,
#             "column": col,
#             "passed": bool(pct <= MAX_NULL_PCT),
#             "value": round(float(pct), 2)
#         })

#     return results

# # ------------------------------------------------------------------
# # View Range Check
# # ------------------------------------------------------------------

# def check_views(df):

#     if "views" not in df.columns:
#         return []

#     negative = (
#         df["views"] < 0
#     ).sum()

#     extreme = (
#         df["views"] > MAX_VIEWS
#     ).sum()

#     return [
#         {
#             "check": "view_range",

#             "table": "clean_statistics",

#             "passed": bool(
#                 negative == 0
#                 and extreme == 0
#             ),

#             "negative": int(negative),

#             "extreme": int(extreme)
#         }
#     ]


# # ------------------------------------------------------------------
# # SNS Alert
# # ------------------------------------------------------------------

# def send_alert(results):

#     if not SNS_ALERT_TOPIC_ARN:

#         logger.warning(
#             "SNS_ALERT_TOPIC_ARN is not configured. "
#             "Skipping alert."
#         )

#         return

#     logger.info(
#         "Sending Data Quality alert to SNS..."
#     )

#     sns.publish(

#         TopicArn=SNS_ALERT_TOPIC_ARN,

#         Subject="YT Pipeline DQ Failed",

#         Message=json.dumps(
#             results,
#             indent=2,
#             default=str
#         )
#     )

#     logger.info(
#         "SNS alert sent successfully."
#     )


# # ------------------------------------------------------------------
# # Lambda Handler
# # ------------------------------------------------------------------

# def lambda_handler(event, context):

#     # --------------------------------------------------------------
#     # IMPORTANT:
#     # This is your actual Glue/Athena Silver database name.
#     # --------------------------------------------------------------

#     database = event.get(
#         "database",
#         "yt-pipeline-silver-devdatta"
#     )

#     tables = event.get(
#         "tables",
#         [
#             "clean_statistics",
#             "clean_reference_data"
#         ]
#     )

#     logger.info(
#         f"Database: {database}"
#     )

#     logger.info(
#         f"Tables: {tables}"
#     )

#     all_results = []

#     overall_passed = True


#     # --------------------------------------------------------------
#     # Check each table
#     # --------------------------------------------------------------

#     for table in tables:

#         logger.info(
#             f"Checking table: {table}"
#         )

#         try:

#             query = (
#                 f'SELECT * '
#                 f'FROM "{table}" '
#                 f'LIMIT 10000'
#             )

#             df = run_athena_query(
#                 query,
#                 database
#             )

#             logger.info(
#                 f"{table}: "
#                 f"{len(df)} rows retrieved"
#             )

#             logger.info(
#                 f"{table} columns: "
#                 f"{list(df.columns)}"
#             )

#         except Exception as e:

#             logger.error(
#                 f"Failed to query {table}: {e}"
#             )

#             overall_passed = False

#             all_results.append({

#                 "table": table,

#                 "passed": False,

#                 "error": str(e)

#             })

#             continue


#         # ----------------------------------------------------------
#         # Run checks
#         # ----------------------------------------------------------

#         checks = []


#         # Row count

#         checks.append(
#             check_row_count(
#                 df,
#                 table
#             )
#         )


#         # Schema

#         checks.append(
#             check_schema(
#                 df,
#                 table
#             )
#         )


#         # Nulls

#         checks.extend(
#             check_nulls(
#                 df,
#                 table
#             )
#         )


#         # Views

#         if table == "clean_statistics":

#             checks.extend(
#                 check_views(df)
#             )


#         # ----------------------------------------------------------
#         # Determine overall status
#         # ----------------------------------------------------------

#         for check in checks:

#             if not check["passed"]:

#                 overall_passed = False


#         all_results.extend(
#             checks
#         )


#     # --------------------------------------------------------------
#     # Send Alert if Failed
#     # --------------------------------------------------------------

#     if not overall_passed:

#         logger.warning(
#             "DATA QUALITY CHECK FAILED"
#         )

#         send_alert(
#             all_results
#         )

#     else:

#         logger.info(
#             "DATA QUALITY CHECK PASSED"
#         )


#     # --------------------------------------------------------------
#     # Response
#     # --------------------------------------------------------------

#     safe_results = []

#     for result in all_results:
#         safe_results.append({
#             "check": str(result.get("check", "")),
#             "table": str(result.get("table", "")),
#             "column": str(result.get("column", "")) if result.get("column") else None,
#             "passed": bool(result.get("passed", False)),
#             "value": result.get("value"),
#             "missing": result.get("missing"),
#             "negative": result.get("negative"),
#             "extreme": result.get("extreme"),
#             "error": str(result.get("error", "")) if result.get("error") else None
#         })

# return {
#     "statusCode": 200,
#     "quality_passed": bool(overall_passed),
#     "results": safe_results
# }














import os
import json
import time
import logging

import boto3
import pandas as pd


# ------------------------------------------------------------------
# Logging
# ------------------------------------------------------------------

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

SNS_ALERT_TOPIC_ARN = os.environ.get(
    "SNS_ALERT_TOPIC_ARN",
    ""
)


# ------------------------------------------------------------------
# Thresholds
# ------------------------------------------------------------------

MIN_ROW_COUNT = 10

MAX_NULL_PCT = 5.0

MAX_VIEWS = 50_000_000_000


# ------------------------------------------------------------------
# Critical Columns
# ------------------------------------------------------------------

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
        "category_name",
        "region"
    ]
}


# ------------------------------------------------------------------
# Run Athena Query
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

    logger.info(
        f"Athena Query ID: {query_id}"
    )

    # --------------------------------------------------------------
    # Wait for Athena
    # --------------------------------------------------------------

    while True:

        response = athena.get_query_execution(
            QueryExecutionId=query_id
        )

        status = (
            response["QueryExecution"]
            ["Status"]
            ["State"]
        )

        logger.info(
            f"Athena query status: {status}"
        )

        if status == "SUCCEEDED":
            break

        if status in [
            "FAILED",
            "CANCELLED"
        ]:

            reason = (
                response["QueryExecution"]
                ["Status"]
                .get(
                    "StateChangeReason",
                    "Unknown"
                )
            )

            raise Exception(
                f"Athena query failed: "
                f"{status} - {reason}"
            )

        time.sleep(2)

    # --------------------------------------------------------------
    # Get Athena result location
    # --------------------------------------------------------------

    result_location = (
        response["QueryExecution"]
        ["ResultConfiguration"]
        ["OutputLocation"]
    )

    logger.info(
        f"Athena result location: "
        f"{result_location}"
    )

    # --------------------------------------------------------------
    # Parse S3 location
    # --------------------------------------------------------------

    result_without_prefix = (
        result_location
        .replace("s3://", "")
    )

    parts = result_without_prefix.split("/")

    bucket = parts[0]

    key = "/".join(parts[1:])

    # --------------------------------------------------------------
    # Read CSV result from S3
    # --------------------------------------------------------------

    obj = s3.get_object(
        Bucket=bucket,
        Key=key
    )

    df = pd.read_csv(
        obj["Body"]
    )

    return df


# ------------------------------------------------------------------
# Row Count Check
# ------------------------------------------------------------------

def check_row_count(df, table):

    count = len(df)

    passed = count >= MIN_ROW_COUNT

    return {

        "check": "row_count",

        "table": table,

        "passed": bool(passed),

        "value": int(count)
    }


# ------------------------------------------------------------------
# Schema Check
# ------------------------------------------------------------------

def check_schema(df, table):

    expected = set(
        CRITICAL_COLUMNS.get(
            table,
            []
        )
    )

    actual = set(
        df.columns
    )

    missing = list(
        expected - actual
    )

    passed = len(missing) == 0

    return {

        "check": "schema",

        "table": table,

        "passed": bool(passed),

        "missing": missing
    }


# ------------------------------------------------------------------
# Null Check
# ------------------------------------------------------------------

def check_nulls(df, table):

    results = []

    required_columns = CRITICAL_COLUMNS.get(
        table,
        []
    )

    for col in required_columns:

        # ----------------------------------------------------------
        # Column missing
        # ----------------------------------------------------------

        if col not in df.columns:

            results.append({

                "check": "nulls",

                "table": table,

                "column": col,

                "passed": False,

                "value": None
            })

            continue

        # ----------------------------------------------------------
        # Calculate null percentage
        # ----------------------------------------------------------

        pct = (
            df[col]
            .isna()
            .mean()
            * 100
        )

        passed = pct <= MAX_NULL_PCT

        results.append({

            "check": "nulls",

            "table": table,

            "column": col,

            "passed": bool(passed),

            "value": round(
                float(pct),
                2
            )
        })

    return results


# ------------------------------------------------------------------
# View Range Check
# ------------------------------------------------------------------

def check_views(df):

    if "views" not in df.columns:

        return []

    # --------------------------------------------------------------
    # Convert views to numeric
    # --------------------------------------------------------------

    views = pd.to_numeric(
        df["views"],
        errors="coerce"
    )

    # --------------------------------------------------------------
    # Negative views
    # --------------------------------------------------------------

    negative = int(
        (views < 0).sum()
    )

    # --------------------------------------------------------------
    # Extremely large views
    # --------------------------------------------------------------

    extreme = int(
        (views > MAX_VIEWS).sum()
    )

    passed = (
        negative == 0
        and extreme == 0
    )

    return [

        {

            "check": "view_range",

            "table": "clean_statistics",

            "passed": bool(passed),

            "negative": negative,

            "extreme": extreme
        }

    ]


# ------------------------------------------------------------------
# Send SNS Alert
# ------------------------------------------------------------------

def send_alert(results):

    if not SNS_ALERT_TOPIC_ARN:

        logger.warning(
            "SNS_ALERT_TOPIC_ARN is not configured. "
            "Skipping alert."
        )

        return

    logger.info(
        "Sending Data Quality alert to SNS..."
    )

    sns.publish(

        TopicArn=SNS_ALERT_TOPIC_ARN,

        Subject="YT Pipeline DQ Failed",

        Message=json.dumps(
            results,
            indent=2,
            default=str
        )
    )

    logger.info(
        "SNS alert sent successfully."
    )


# ------------------------------------------------------------------
# Lambda Handler
# ------------------------------------------------------------------

def lambda_handler(event, context):

    # --------------------------------------------------------------
    # Database
    # --------------------------------------------------------------

    database = event.get(
        "database",
        "yt-pipeline-silver-devdatta"
    )

    # --------------------------------------------------------------
    # Tables
    # --------------------------------------------------------------

    tables = event.get(

        "tables",

        [
            "clean_statistics",
            "clean_reference_data"
        ]
    )

    logger.info(
        f"Database: {database}"
    )

    logger.info(
        f"Tables: {tables}"
    )

    # --------------------------------------------------------------
    # Result containers
    # --------------------------------------------------------------

    all_results = []

    overall_passed = True

    # ==============================================================
    # CHECK EACH TABLE
    # ==============================================================

    for table in tables:

        logger.info(
            f"Checking table: {table}"
        )

        # ----------------------------------------------------------
        # Query table
        # ----------------------------------------------------------

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

            logger.info(
                f"{table}: "
                f"{len(df)} rows retrieved"
            )

            logger.info(
                f"{table} columns: "
                f"{list(df.columns)}"
            )

        except Exception as e:

            logger.error(
                f"Failed to query "
                f"{table}: {e}"
            )

            overall_passed = False

            all_results.append({

                "check": "athena_query",

                "table": table,

                "passed": False,

                "error": str(e)
            })

            continue

        # ----------------------------------------------------------
        # Run checks
        # ----------------------------------------------------------

        checks = []

        # ----------------------------------------------------------
        # Row count
        # ----------------------------------------------------------

        row_count_result = check_row_count(
            df,
            table
        )

        checks.append(
            row_count_result
        )

        # ----------------------------------------------------------
        # Schema
        # ----------------------------------------------------------

        schema_result = check_schema(
            df,
            table
        )

        checks.append(
            schema_result
        )

        # ----------------------------------------------------------
        # Null checks
        # ----------------------------------------------------------

        null_results = check_nulls(
            df,
            table
        )

        checks.extend(
            null_results
        )

        # ----------------------------------------------------------
        # View checks
        # ----------------------------------------------------------

        if table == "clean_statistics":

            view_results = check_views(
                df
            )

            checks.extend(
                view_results
            )

        # ----------------------------------------------------------
        # Evaluate checks
        # ----------------------------------------------------------

        for check in checks:

            if not bool(
                check.get(
                    "passed",
                    False
                )
            ):

                overall_passed = False

        # ----------------------------------------------------------
        # Store results
        # ----------------------------------------------------------

        all_results.extend(
            checks
        )

    # ==============================================================
    # FINAL DATA QUALITY STATUS
    # ==============================================================

    if overall_passed:

        logger.info(
            "DATA QUALITY CHECK PASSED"
        )

    else:

        logger.warning(
            "DATA QUALITY CHECK FAILED"
        )

        send_alert(
            all_results
        )

    # ==============================================================
    # BUILD SAFE JSON RESPONSE
    # ==============================================================

    safe_results = []

    for result in all_results:

        safe_result = {

            "check": str(
                result.get(
                    "check",
                    ""
                )
            ),

            "table": str(
                result.get(
                    "table",
                    ""
                )
            ),

            "column": (
                str(
                    result.get(
                        "column"
                    )
                )
                if result.get(
                    "column"
                ) is not None
                else None
            ),

            "passed": bool(
                result.get(
                    "passed",
                    False
                )
            ),

            "value": result.get(
                "value"
            ),

            "missing": result.get(
                "missing"
            ),

            "negative": result.get(
                "negative"
            ),

            "extreme": result.get(
                "extreme"
            ),

            "error": (
                str(
                    result.get(
                        "error"
                    )
                )
                if result.get(
                    "error"
                ) is not None
                else None
            )
        }

        safe_results.append(
            safe_result
        )

    # ==============================================================
    # FINAL LAMBDA RESPONSE
    # ==============================================================

    return {

        "statusCode": 200,

        "quality_passed": bool(
            overall_passed
        ),

        "results": safe_results
    }
