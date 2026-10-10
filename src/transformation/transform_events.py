
import logging

from pyspark.sql import DataFrame
from pyspark.sql.functions import (
    col,
    trim,
    upper,
    to_date,
    year,
    month,
    dayofmonth,
    hour,
    when,
    count,
)

DATABASE = "ecommerce_data_platform"
TABLE = "events"
OUTPUT_PATH = (
    "s3://ecommerce-data-platform-version1/refined/events/"
)

logger = logging.getLogger("RefinedEvents")

REQUIRED_COLUMNS = [
    "event_id",
    "event_type",
    "source",
    "payload",
    "customer_id",
    "order_id",
    "product_id",
    "event_timestamp",
]


def validate_columns(df: DataFrame) -> None:
    missing = sorted(set(REQUIRED_COLUMNS) - set(df.columns))
    if missing:
        raise ValueError(
            f"Events table is missing required columns: {missing}"
        )


def run_transformation(glue_context) -> dict:
    logger.info("Starting events refined transformation")

    # 1. Read raw events from Glue Catalog.
    source_dyf = glue_context.create_dynamic_frame.from_catalog(
        database=DATABASE,
        table_name=TABLE,
    )
    events_df = source_dyf.toDF()

    validate_columns(events_df)

    source_count = events_df.count()
    logger.info("Raw event rows: %s", source_count)

    # 2. Standardize fields.
    events_df = (
        events_df
        .withColumn("event_id", trim(col("event_id").cast("string")))
        .withColumn("event_type", upper(trim(col("event_type"))))
        .withColumn("source", upper(trim(col("source"))))
        .withColumn("payload", trim(col("payload")))
        .withColumn("customer_id", trim(col("customer_id").cast("string")))
        .withColumn("order_id", trim(col("order_id").cast("string")))
        .withColumn("product_id", trim(col("product_id").cast("string")))
        .withColumn(
            "event_timestamp",
            col("event_timestamp").cast("timestamp"),
        )
    )

    # 3. Derive event date and time dimensions.
    events_df = (
        events_df
        .withColumn("event_date", to_date(col("event_timestamp")))
        .withColumn("event_year", year(col("event_timestamp")))
        .withColumn("event_month", month(col("event_timestamp")))
        .withColumn("event_day", dayofmonth(col("event_timestamp")))
        .withColumn("event_hour", hour(col("event_timestamp")))
    )

    # 4. Categorize event types.
    events_df = events_df.withColumn(
        "event_category",
        when(
            col("event_type").isin("PRODUCT_VIEWED", "ADD_TO_CART"),
            "BROWSING",
        )
        .when(col("event_type") == "ORDER_CREATED", "ORDER")
        .when(
            col("event_type").isin("PAYMENT_COMPLETED", "PAYMENT_FAILED"),
            "PAYMENT",
        )
        .when(
            col("event_type").isin("ORDER_SHIPPED", "ORDER_DELIVERED"),
            "FULFILLMENT",
        )
        .otherwise("OTHER"),
    )

    # 5. Flag invalid data without silently deleting records.
    events_df = events_df.withColumn(
        "event_data_quality_status",
        when(
            col("event_id").isNull()
            | (col("event_id") == "")
            | col("event_type").isNull()
            | (col("event_type") == "")
            | col("event_timestamp").isNull(),
            "REVIEW",
        ).otherwise("VALID"),
    )

    # 6. Detect duplicate event IDs.
    duplicate_ids = (
        events_df
        .filter(col("event_id").isNotNull() & (col("event_id") != ""))
        .groupBy("event_id")
        .agg(count("*").alias("record_count"))
        .filter(col("record_count") > 1)
        .select("event_id")
    )

    duplicate_id_count = duplicate_ids.count()

    if duplicate_id_count:
        logger.warning(
            "Found %s event IDs with duplicate records; "
            "records are retained for review.",
            duplicate_id_count,
        )

        events_df = events_df.join(
            duplicate_ids.withColumn("_duplicate_event_id", col("event_id"))
            .drop("event_id"),
            events_df["event_id"] == col("_duplicate_event_id"),
            "left",
        ).withColumn(
            "event_data_quality_status",
            when(
                col("_duplicate_event_id").isNotNull(),
                "REVIEW",
            ).otherwise(col("event_data_quality_status")),
        ).drop("_duplicate_event_id")

    # 7. Log distributions.
    events_df.groupBy("event_data_quality_status").count().show()
    events_df.groupBy("event_category").count().show()
    events_df.groupBy("event_type").count().show()

    # 8. Write refined Parquet data.
    events_df.write.mode("overwrite").parquet(OUTPUT_PATH)

    logger.info("Refined events written to %s", OUTPUT_PATH)

    # 9. Verify output.
    refined_df = events_df.sparkSession.read.parquet(OUTPUT_PATH)
    output_count = refined_df.count()

    if output_count != source_count:
        logger.warning(
            "Event row count changed: source=%s, refined=%s",
            source_count,
            output_count,
        )

    logger.info(
        "Events transformation completed: %s rows",
        output_count,
    )

    return {
        "dataset": "events",
        "source_count": source_count,
        "output_count": output_count,
        "output_path": OUTPUT_PATH,
        "status": "SUCCESS",
    }
