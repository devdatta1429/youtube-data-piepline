import sys

from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from awsglue.dynamicframe import DynamicFrame

from pyspark.sql import functions as F
from pyspark.sql.window import Window


"""
Glue Job: Silver → Gold (Analytics Aggregations)

Reads cleansed statistics and reference data from Silver,
joins them, and produces business-level aggregations in Gold.

Gold tables produced:

1. trending_analytics
   Daily trending summaries per region

2. channel_analytics
   Channel performance metrics

3. category_analytics
   Category-level trends over time
"""


# ══════════════════════════════════════════════════════════════════════════════
# JOB SETUP
# ══════════════════════════════════════════════════════════════════════════════

args = getResolvedOptions(
    sys.argv,
    [
        "JOB_NAME",
        "silver_database",
        "gold_bucket",
        "gold_database",
    ],
)

sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session

job = Job(glueContext)
job.init(args["JOB_NAME"], args)

logger = glueContext.get_logger()

SILVER_DB = args["silver_database"]
GOLD_BUCKET = args["gold_bucket"]
GOLD_DB = args["gold_database"]


logger.info("==============================================")
logger.info("Starting Silver → Gold Analytics Job")
logger.info("==============================================")

logger.info(f"Silver Database: {SILVER_DB}")
logger.info(f"Gold Database: {GOLD_DB}")
logger.info(f"Gold Bucket: {GOLD_BUCKET}")


# ══════════════════════════════════════════════════════════════════════════════
# READ SILVER STATISTICS
# ══════════════════════════════════════════════════════════════════════════════

logger.info("Reading Silver statistics table...")

stats_dyf = glueContext.create_dynamic_frame.from_catalog(
    database=SILVER_DB,
    table_name="clean_statistics",
    transformation_ctx="stats",
)

stats_df = stats_dyf.toDF()

logger.info(
    f"Statistics columns: {stats_df.columns}"
)

logger.info(
    f"Statistics records: {stats_df.count()}"
)


# ══════════════════════════════════════════════════════════════════════════════
# READ SILVER REFERENCE DATA
# ══════════════════════════════════════════════════════════════════════════════

logger.info("Reading Silver reference data...")

try:

    ref_dyf = glueContext.create_dynamic_frame.from_catalog(
        database=SILVER_DB,
        table_name="clean_reference_data",
        transformation_ctx="ref",
    )

    ref_df = ref_dyf.toDF()

    logger.info(
        f"Reference columns: {ref_df.columns}"
    )

    logger.info(
        f"Reference records: {ref_df.count()}"
    )


    # ══════════════════════════════════════════════════════════════════════════
    # BUILD CATEGORY LOOKUP
    # ══════════════════════════════════════════════════════════════════════════

    logger.info("Building category lookup...")

    category_lookup = (
        ref_df
        .select(
            F.col("category_id")
                .cast("long")
                .alias("category_id"),

            F.col("category_name")
                .cast("string")
                .alias("category_name"),

            F.lower(
                F.col("region")
            ).alias("region")
        )
        .dropDuplicates(
            ["category_id", "region"]
        )
    )

    logger.info(
        f"Category lookup entries: {category_lookup.count()}"
    )


    # ══════════════════════════════════════════════════════════════════════════
    # PREPARE STATISTICS FOR JOIN
    # ══════════════════════════════════════════════════════════════════════════

    logger.info("Preparing statistics join keys...")

    # _processed_at and _job_name are metadata fields.
    # They are not required by any Gold aggregation.
    #
    # Removing them also avoids carrying the timestamp[ns] field
    # through the Spark join.

    stats_df = (
        stats_df
        .drop(
            "_processed_at",
            "_job_name"
        )
        .withColumn(
            "category_id",
            F.col("category_id").cast("long")
        )
        .withColumn(
            "region",
            F.lower(F.col("region"))
        )
    )

    logger.info(
        f"Prepared statistics columns: {stats_df.columns}"
    )


    # ══════════════════════════════════════════════════════════════════════════
    # JOIN STATISTICS WITH CATEGORY REFERENCE
    # ══════════════════════════════════════════════════════════════════════════

    logger.info(
        "Joining statistics with category reference data..."
    )

    stats_df = (
        stats_df.alias("s")
        .join(
            F.broadcast(
                category_lookup.alias("c")
            ),
            (
                (F.col("s.category_id") == F.col("c.category_id"))
                &
                (F.col("s.region") == F.col("c.region"))
            ),
            "left"
        )
        .select(
            "s.*",
            F.col("c.category_name").alias("category_name")
        )
    )

    logger.info(
        "Statistics successfully joined with category reference data."
    )


    # ══════════════════════════════════════════════════════════════════════════
    # FORCE JOIN EVALUATION
    # ══════════════════════════════════════════════════════════════════════════

    logger.info(
        "Validating joined statistics DataFrame..."
    )

    joined_count = stats_df.count()

    logger.info(
        f"Joined statistics records: {joined_count}"
    )


except Exception as e:

    logger.error(
        f"Could not load or join reference data: {e}"
    )

    raise


# ══════════════════════════════════════════════════════════════════════════════
# CHECK UNKNOWN CATEGORIES
# ══════════════════════════════════════════════════════════════════════════════

logger.info(
    "Checking for statistics without category mappings..."
)

unknown_count = (
    stats_df
    .filter(
        F.col("category_name").isNull()
    )
    .count()
)

logger.info(
    f"Rows with missing category_name after join: {unknown_count}"
)

if unknown_count > 0:

    logger.info(
        f"WARNING: {unknown_count} rows could not be mapped to a category."
    )

# Replace remaining NULL category names

stats_df = stats_df.fillna(
    "Unknown",
    subset=["category_name"]
)

logger.info(
    "Category mapping validation complete."
)


# ══════════════════════════════════════════════════════════════════════════════
# GOLD TABLE 1: TRENDING ANALYTICS
# Daily summaries per region
# ══════════════════════════════════════════════════════════════════════════════

logger.info(
    "Building Gold: trending_analytics..."
)

trending = (
    stats_df
    .groupBy(
        "region",
        "trending_date_parsed"
    )
    .agg(
        F.count("video_id")
            .alias("total_videos"),

        F.sum("views")
            .alias("total_views"),

        F.sum("likes")
            .alias("total_likes"),

        F.sum("dislikes")
            .alias("total_dislikes"),

        F.sum("comment_count")
            .alias("total_comments"),

        F.avg("views")
            .alias("avg_views_per_video"),

        F.avg("like_ratio")
            .alias("avg_like_ratio"),

        F.avg("engagement_rate")
            .alias("avg_engagement_rate"),

        F.max("views")
            .alias("max_views"),

        F.countDistinct("channel_title")
            .alias("unique_channels"),

        F.countDistinct("category_id")
            .alias("unique_categories"),
    )
)


trending = trending.withColumn(
    "_aggregated_at",
    F.current_timestamp()
)


trending_path = (
    f"{GOLD_BUCKET}/youtube/trending_analytics/"
)


trending_dyf = DynamicFrame.fromDF(
    trending,
    glueContext,
    "trending"
)


sink1 = glueContext.getSink(
    connection_type="s3",
    path=trending_path,
    enableUpdateCatalog=True,
    updateBehavior="UPDATE_IN_DATABASE",
    partitionKeys=["region"],
)

sink1.setCatalogInfo(
    catalogDatabase=GOLD_DB,
    catalogTableName="trending_analytics",
)

sink1.setFormat(
    "glueparquet",
    compression="snappy"
)

sink1.writeFrame(
    trending_dyf
)


logger.info(
    f"Written {trending.count()} rows to trending_analytics"
)


# ══════════════════════════════════════════════════════════════════════════════
# GOLD TABLE 2: CHANNEL ANALYTICS
# ══════════════════════════════════════════════════════════════════════════════

logger.info(
    "Building Gold: channel_analytics..."
)


channel = (
    stats_df
    .groupBy(
        "channel_title",
        "region"
    )
    .agg(
        F.countDistinct("video_id")
            .alias("total_videos"),

        F.sum("views")
            .alias("total_views"),

        F.sum("likes")
            .alias("total_likes"),

        F.sum("comment_count")
            .alias("total_comments"),

        F.avg("views")
            .alias("avg_views_per_video"),

        F.avg("engagement_rate")
            .alias("avg_engagement_rate"),

        F.max("views")
            .alias("peak_views"),

        F.count("trending_date_parsed")
            .alias("times_trending"),

        F.min("trending_date_parsed")
            .alias("first_trending"),

        F.max("trending_date_parsed")
            .alias("last_trending"),

        F.collect_set("category_name")
            .alias("categories"),
    )
)


# Rank channels by total views within each region

window_rank = (
    Window
    .partitionBy("region")
    .orderBy(
        F.col("total_views").desc()
    )
)


channel = channel.withColumn(
    "rank_in_region",
    F.row_number().over(window_rank)
)


channel = channel.withColumn(
    "_aggregated_at",
    F.current_timestamp()
)


channel_path = (
    f"{GOLD_BUCKET}/youtube/channel_analytics/"
)


channel_dyf = DynamicFrame.fromDF(
    channel,
    glueContext,
    "channel"
)


sink2 = glueContext.getSink(
    connection_type="s3",
    path=channel_path,
    enableUpdateCatalog=True,
    updateBehavior="UPDATE_IN_DATABASE",
    partitionKeys=["region"],
)

sink2.setCatalogInfo(
    catalogDatabase=GOLD_DB,
    catalogTableName="channel_analytics",
)

sink2.setFormat(
    "glueparquet",
    compression="snappy"
)

sink2.writeFrame(
    channel_dyf
)


logger.info(
    f"Written {channel.count()} rows to channel_analytics"
)


# ══════════════════════════════════════════════════════════════════════════════
# GOLD TABLE 3: CATEGORY ANALYTICS
# Trend over time
# ══════════════════════════════════════════════════════════════════════════════

logger.info(
    "Building Gold: category_analytics..."
)


category = (
    stats_df
    .groupBy(
        "category_name",
        "category_id",
        "region",
        "trending_date_parsed"
    )
    .agg(
        F.count("video_id")
            .alias("video_count"),

        F.sum("views")
            .alias("total_views"),

        F.sum("likes")
            .alias("total_likes"),

        F.sum("comment_count")
            .alias("total_comments"),

        F.avg("engagement_rate")
            .alias("avg_engagement_rate"),

        F.countDistinct("channel_title")
            .alias("unique_channels"),
    )
)


# ══════════════════════════════════════════════════════════════════════════════
# CATEGORY SHARE OF VIEWS
# Per region per day
# ══════════════════════════════════════════════════════════════════════════════

window_total = (
    Window
    .partitionBy(
        "region",
        "trending_date_parsed"
    )
)


category = category.withColumn(
    "view_share_pct",
    F.round(
        (
            F.col("total_views")
            /
            F.sum("total_views").over(window_total)
            *
            100
        ),
        2
    )
)


category = category.withColumn(
    "_aggregated_at",
    F.current_timestamp()
)


category_path = (
    f"{GOLD_BUCKET}/youtube/category_analytics/"
)


category_dyf = DynamicFrame.fromDF(
    category,
    glueContext,
    "category"
)


sink3 = glueContext.getSink(
    connection_type="s3",
    path=category_path,
    enableUpdateCatalog=True,
    updateBehavior="UPDATE_IN_DATABASE",
    partitionKeys=["region"],
)

sink3.setCatalogInfo(
    catalogDatabase=GOLD_DB,
    catalogTableName="category_analytics",
)

sink3.setFormat(
    "glueparquet",
    compression="snappy"
)

sink3.writeFrame(
    category_dyf
)


logger.info(
    f"Written {category.count()} rows to category_analytics"
)


# ══════════════════════════════════════════════════════════════════════════════
# JOB COMPLETE
# ══════════════════════════════════════════════════════════════════════════════

logger.info(
    "=============================================="
)

logger.info(
    "Gold layer build complete."
)

logger.info(
    "=============================================="
)


job.commit()
