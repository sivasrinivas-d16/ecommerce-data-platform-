
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
    when,
    count,
)

DATABASE = "ecommerce_data_platform"
TABLE = "payments"
OUTPUT_PATH = (
    "s3://ecommerce-data-platform-version1/refined/payments/"
)

logger = logging.getLogger("RefinedPayments")

REQUIRED_COLUMNS = [
    "payment_id",
    "payment_method",
    "payment_status",
    "transaction_reference",
    "payment_timestamp",
    "payment_amount",
]


def validate_columns(df: DataFrame) -> None:
    missing = sorted(set(REQUIRED_COLUMNS) - set(df.columns))
    if missing:
        raise ValueError(
            f"Payments table is missing required columns: {missing}"
        )


def run_transformation(glue_context) -> dict:
    logger.info("Starting payments refined transformation")

    # 1. Read from Glue Catalog.
    source_dyf = glue_context.create_dynamic_frame.from_catalog(
        database=DATABASE,
        table_name=TABLE,
    )
    payments_df = source_dyf.toDF()

    validate_columns(payments_df)

    source_count = payments_df.count()
    logger.info("Raw payment rows: %s", source_count)

    # 2. Standardize payment fields.
    payments_df = (
        payments_df
        .withColumn("payment_id", trim(col("payment_id").cast("string")))
        .withColumn(
            "payment_method",
            upper(trim(col("payment_method"))),
        )
        .withColumn(
            "payment_status",
            upper(trim(col("payment_status"))),
        )
        .withColumn(
            "transaction_reference",
            trim(col("transaction_reference")),
        )
    )

    # 3. Cast timestamp and amount.
    payments_df = (
        payments_df
        .withColumn(
            "payment_timestamp",
            col("payment_timestamp").cast("timestamp"),
        )
        .withColumn(
            "payment_amount",
            col("payment_amount").cast("decimal(18,2)"),
        )
    )

    # 4. Derive payment date dimensions.
    payments_df = (
        payments_df
        .withColumn("payment_date", to_date(col("payment_timestamp")))
        .withColumn("payment_year", year(col("payment_timestamp")))
        .withColumn("payment_month", month(col("payment_timestamp")))
        .withColumn("payment_day", dayofmonth(col("payment_timestamp")))
    )

    # 5. Classify payment amounts. Null/negative amounts are not
    # incorrectly classified as premium-value payments.
    payments_df = payments_df.withColumn(
        "payment_value_category",
        when(col("payment_amount").isNull(), "UNKNOWN")
        .when(col("payment_amount") < 0, "INVALID")
        .when(col("payment_amount") < 1000, "LOW_VALUE")
        .when(col("payment_amount") <= 10000, "MEDIUM_VALUE")
        .when(col("payment_amount") <= 50000, "HIGH_VALUE")
        .otherwise("PREMIUM_VALUE"),
    )

    # 6. Map payment statuses.
    payments_df = payments_df.withColumn(
        "payment_result",
        when(col("payment_status") == "PAID", "SUCCESS")
        .when(col("payment_status") == "FAILED", "FAILED")
        .when(col("payment_status") == "PENDING", "PENDING")
        .when(col("payment_status") == "REFUNDED", "REFUNDED")
        .otherwise("UNKNOWN"),
    )

    # 7. Flag invalid data without deleting records.
    payments_df = payments_df.withColumn(
        "payment_data_quality_status",
        when(
            col("payment_id").isNull()
            | (col("payment_id") == "")
            | col("payment_timestamp").isNull()
            | col("payment_amount").isNull()
            | (col("payment_amount") < 0),
            "REVIEW",
        ).otherwise("VALID"),
    )

    # 8. Detect duplicate payment IDs.
    duplicate_ids = (
        payments_df
        .filter(col("payment_id").isNotNull() & (col("payment_id") != ""))
        .groupBy("payment_id")
        .agg(count("*").alias("record_count"))
        .filter(col("record_count") > 1)
        .select("payment_id")
    )

    duplicate_id_count = duplicate_ids.count()

    if duplicate_id_count:
        logger.warning(
            "Found %s payment IDs with duplicate records; "
            "records are retained for review.",
            duplicate_id_count,
        )

        payments_df = payments_df.join(
            duplicate_ids.withColumn("_duplicate_payment_id", col("payment_id"))
            .drop("payment_id"),
            payments_df["payment_id"] == col("_duplicate_payment_id"),
            "left",
        ).withColumn(
            "payment_data_quality_status",
            when(
                col("_duplicate_payment_id").isNotNull(),
                "REVIEW",
            ).otherwise(col("payment_data_quality_status")),
        ).drop("_duplicate_payment_id")

    # 9. Log summary distributions.
    payments_df.groupBy("payment_data_quality_status").count().show()
    payments_df.groupBy("payment_value_category").count().show()
    payments_df.groupBy("payment_result").count().show()

    # 10. Write refined Parquet data.
    payments_df.write.mode("overwrite").parquet(OUTPUT_PATH)

    logger.info("Refined payments written to %s", OUTPUT_PATH)

    # 11. Verify output.
    refined_df = payments_df.sparkSession.read.parquet(OUTPUT_PATH)
    output_count = refined_df.count()

    if output_count != source_count:
        logger.warning(
            "Payment row count changed: source=%s, refined=%s",
            source_count,
            output_count,
        )

    logger.info(
        "Payments transformation completed: %s rows",
        output_count,
    )

    return {
        "dataset": "payments",
        "source_count": source_count,
        "output_count": output_count,
        "output_path": OUTPUT_PATH,
        "status": "SUCCESS",
    }
