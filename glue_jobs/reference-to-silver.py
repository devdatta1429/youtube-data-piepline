import sys

from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from awsglue.dynamicframe import DynamicFrame

from pyspark.sql import functions as F

# ---------------------------------------------------
# Job Setup
# ---------------------------------------------------

args = getResolvedOptions(
    sys.argv,
    [
        "JOB_NAME",
        "bronze_database",
        "bronze_reference_table",
        "silver_database",
        "silver_bucket"
    ]
)

sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session

job = Job(glueContext)
job.init(args["JOB_NAME"], args)

logger = glueContext.get_logger()

BRONZE_DB = args["bronze_database"]
BRONZE_TABLE = args["bronze_reference_table"]

SILVER_DB = args["silver_database"]
SILVER_BUCKET = args["silver_bucket"]

SILVER_PATH = (
    f"s3://{SILVER_BUCKET}/youtube/reference_data/"
)

# ---------------------------------------------------
# Read Bronze Reference Data
# ---------------------------------------------------

logger.info(
    f"Reading {BRONZE_DB}.{BRONZE_TABLE}"
)

source_dyf = glueContext.create_dynamic_frame.from_catalog(
    database=BRONZE_DB,
    table_name=BRONZE_TABLE
)

df = source_dyf.toDF()

logger.info(
    f"Rows Read: {df.count()}"
)

logger.info(
    f"Columns: {df.columns}"
)

# ---------------------------------------------------
# Extract Category Fields
# ---------------------------------------------------

if "id" not in df.columns:
    raise Exception(
        "Column 'id' not found in reference table"
    )

if "snippet.title" in df.columns:

    category_df = df.select(
        F.col("id")
        .cast("long")
        .alias("category_id"),

        F.col("`snippet.title`")
        .alias("category_name"),

        F.lower(
            F.col("region")
        ).alias("region")
    )

elif "snippet_title" in df.columns:

    category_df = df.select(
        F.col("id")
        .cast("long")
        .alias("category_id"),

        F.col("snippet_title")
        .alias("category_name"),

        F.lower(
            F.col("region")
        ).alias("region")
    )

else:

    raise Exception(
        f"Could not find category title column. Columns={df.columns}"
    )

# ---------------------------------------------------
# Cleaning
# ---------------------------------------------------

category_df = category_df.filter(
    F.col("category_id").isNotNull()
)

category_df = category_df.filter(
    F.col("category_name").isNotNull()
)

category_df = category_df.dropDuplicates(
    ["category_id", "region"]
)

category_df = category_df.withColumn(
    "_processed_at",
    F.current_timestamp()
)

logger.info(
    f"Clean Rows: {category_df.count()}"
)

# ---------------------------------------------------
# Write Silver
# ---------------------------------------------------

output_dyf = DynamicFrame.fromDF(
    category_df,
    glueContext,
    "output_dyf"
)

sink = glueContext.getSink(
    connection_type="s3",
    path=SILVER_PATH,
    enableUpdateCatalog=True,
    updateBehavior="UPDATE_IN_DATABASE",
    partitionKeys=["region"]
)

sink.setCatalogInfo(
    catalogDatabase=SILVER_DB,
    catalogTableName="clean_reference_data"
)

sink.setFormat(
    "glueparquet",
    compression="snappy"
)

sink.writeFrame(output_dyf)

logger.info(
    f"Written to {SILVER_PATH}"
)

job.commit()