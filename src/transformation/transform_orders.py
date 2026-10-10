
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
    round as spark_round,
    when,
    count,
)

DATABASE = "ecommerce_data_platform"
TABLE = "orders"
OUTPUT_PATH = (
    "s3://ecommerce-data-platform-version1/refined/orders/"
)

logger = logging.getLogger("RefinedOrders")

REQUIRED_COLUMNS = [
    "order_id",
    "customer_id",
    "product_id",
    "order_timestamp",
    "quantity",
    "unit_price",
    "order_amount",
    "order_status",
    "payment_status",
]


def validate_columns(df: DataFrame) -> None:
    missing = sorted(set(REQUIRED_COLUMNS) - set(df.columns))
    if missing:
        raise ValueError(
            f"Orders table is missing required columns: {missing}"
        )


def run_transformation(glue_context) -> dict:
    logger.info("Starting orders refined transformation")

    source_dyf = glue_context.create_dynamic_frame.from_catalog(
        database=DATABASE,
        table_name=TABLE,
    )
    orders_df = source_dyf.toDF()

    validate_columns(orders_df)

    source_count = orders_df.count()
    logger.info("Raw order rows: %s", source_count)

    # Standardize identifiers and status values.
    orders_df = (
        orders_df
        .withColumn("order_id", trim(col("order_id").cast("string")))
        .withColumn("customer_id", trim(col("customer_id").cast("string")))
        .withColumn("product_id", trim(col("product_id").cast("string")))
        .withColumn("order_status", upper(trim(col("order_status"))))
        .withColumn("payment_status", upper(trim(col("payment_status"))))
    )

    # Normalize timestamps and numeric values.
    orders_df = (
        orders_df
        .withColumn("order_timestamp", col("order_timestamp").cast("timestamp"))
        .withColumn("quantity", col("quantity").cast("long"))
        .withColumn("unit_price", col("unit_price").cast("decimal(18,2)"))
        .withColumn("order_amount", col("order_amount").cast("decimal(18,2)"))
    )

    # Derive order date dimensions.
    orders_df = (
        orders_df
        .withColumn("order_date", to_date(col("order_timestamp")))
        .withColumn("order_year", year(col("order_timestamp")))
        .withColumn("order_month", month(col("order_timestamp")))
        .withColumn("order_day", dayofmonth(col("order_timestamp")))
    )

    # Calculate expected amount using decimal arithmetic.
    orders_df = orders_df.withColumn(
        "calculated_order_amount",
        spark_round(
            col("quantity") * col("unit_price"),
            2,
        ).cast("decimal(18,2)"),
    )

    orders_df = orders_df.withColumn(
        "order_amount_valid",
        when(
            col("quantity").isNull()
            | (col("quantity") <= 0)
            | col("unit_price").isNull()
            | (col("unit_price") < 0)
            | col("order_amount").isNull()
            | col("calculated_order_amount").isNull(),
            None,
        ).otherwise(
            col("order_amount") == col("calculated_order_amount")
        ),
    )

    # Flag invalid data without dropping rows.
    orders_df = orders_df.withColumn(
        "order_data_quality_status",
        when(
            col("order_id").isNull()
            | (col("order_id") == "")
            | col("customer_id").isNull()
            | (col("customer_id") == "")
            | col("order_timestamp").isNull()
            | col("quantity").isNull()
            | (col("quantity") <= 0)
            | col("unit_price").isNull()
            | (col("unit_price") < 0)
            | col("order_amount").isNull()
            | (col("order_amount") < 0),
            "REVIEW",
        )
        .when(col("order_amount_valid") == False, "REVIEW")
        .otherwise("VALID"),
    )

    # Business flags.
    orders_df = (
        orders_df
        .withColumn(
            "is_completed_order",
            when(col("order_status").isNull(), None)
            .otherwise(col("order_status") == "COMPLETED"),
        )
        .withColumn(
            "is_paid_order",
            when(col("payment_status").isNull(), None)
            .otherwise(col("payment_status") == "PAID"),
        )
        .withColumn(
            "net_order_amount",
            when(
                col("order_status") == "COMPLETED",
                col("order_amount"),
            ).otherwise(0).cast("decimal(18,2)"),
        )
    )

    # Detect duplicate order IDs without arbitrarily discarding rows.
    duplicate_ids = (
        orders_df
        .filter(col("order_id").isNotNull() & (col("order_id") != ""))
        .groupBy("order_id")
        .agg(count("*").alias("record_count"))
        .filter(col("record_count") > 1)
        .select("order_id")
    )

    duplicate_id_count = duplicate_ids.count()

    if duplicate_id_count:
        logger.warning(
            "Found %s order IDs with duplicate records; "
            "records are retained for review.",
            duplicate_id_count,
        )

        orders_df = orders_df.join(
            duplicate_ids.withColumn("_duplicate_order_id", col("order_id"))
            .drop("order_id"),
            orders_df["order_id"] == col("_duplicate_order_id"),
            "left",
        ).withColumn(
            "order_data_quality_status",
            when(
                col("_duplicate_order_id").isNotNull(),
                "REVIEW",
            ).otherwise(col("order_data_quality_status")),
        ).drop("_duplicate_order_id")

    orders_df.groupBy("order_data_quality_status").count().show()
    orders_df.groupBy("order_status").count().show()
    orders_df.groupBy("payment_status").count().show()

    # Write partitioned Parquet output.
    (
        orders_df.write
        .mode("overwrite")
        .partitionBy("order_year", "order_month")
        .parquet(OUTPUT_PATH)
    )

    logger.info("Refined orders written to %s", OUTPUT_PATH)

    # Verify output.
    refined_df = orders_df.sparkSession.read.parquet(OUTPUT_PATH)
    output_count = refined_df.count()

    if output_count != source_count:
        logger.warning(
            "Order row count changed: source=%s, refined=%s",
            source_count,
            output_count,
        )

    logger.info(
        "Orders transformation completed: %s rows",
        output_count,
    )

    return {
        "dataset": "orders",
        "source_count": source_count,
        "output_count": output_count,
        "output_path": OUTPUT_PATH,
        "status": "SUCCESS",
    }
