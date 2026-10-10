
import logging
from datetime import date

from pyspark.sql import DataFrame
from pyspark.sql.functions import (
    col,
    trim,
    lower,
    to_date,
    year,
    month,
    dayofmonth,
    datediff,
    current_date,
    when,
    count,
)

DATABASE = "ecommerce_data_platform"
TABLE = "customers"
OUTPUT_PATH = (
    "s3://ecommerce-data-platform-version1/refined/customers/"
)

logger = logging.getLogger("RefinedCustomers")

REQUIRED_COLUMNS = [
    "customer_id",
    "name",
    "email",
    "signup_date",
    "city",
    "state",
    "country",
]


def validate_columns(df: DataFrame) -> None:
    """Fail clearly when required source columns are missing."""
    missing = sorted(set(REQUIRED_COLUMNS) - set(df.columns))

    if missing:
        raise ValueError(
            f"Customers table is missing required columns: {missing}"
        )


def run_transformation(glue_context) -> dict:
    """
    Transform raw customers and write refined Parquet data.

    The caller supplies the existing GlueContext.
    Returns basic execution metadata for orchestration.
    """

    logger.info("Starting customers refined transformation")

    # 1. Read raw customers from Glue Catalog.
    source_dyf = glue_context.create_dynamic_frame.from_catalog(
        database=DATABASE,
        table_name=TABLE,
    )
    customers_df = source_dyf.toDF()

    validate_columns(customers_df)

    source_count = customers_df.count()
    logger.info("Raw customer rows: %s", source_count)

    # 2. Standardize text fields.
    customers_df = (
        customers_df
        .withColumn("customer_id", trim(col("customer_id").cast("string")))
        .withColumn("name", trim(col("name")))
        .withColumn("email", lower(trim(col("email"))))
        .withColumn("city", trim(col("city")))
        .withColumn("state", trim(col("state")))
        .withColumn("country", trim(col("country")))
    )

    # Convert supported signup-date formats.
    # Supports the format in the original transformation and ISO dates.
    raw_signup_date = trim(col("signup_date").cast("string"))

    customers_df = customers_df.withColumn(
        "signup_date",
        when(
            raw_signup_date.rlike(r"^\d{2}-\d{2}-\d{4}$"),
            to_date(raw_signup_date, "dd-MM-yyyy"),
        ).otherwise(to_date(raw_signup_date)),
    )

    # 3. Flag incomplete or invalid records.
    customers_df = customers_df.withColumn(
        "customer_data_quality_status",
        when(
            col("customer_id").isNull()
            | (col("customer_id") == "")
            | col("name").isNull()
            | (col("name") == "")
            | col("email").isNull()
            | (col("email") == "")
            | col("signup_date").isNull(),
            "REVIEW",
        )
        .when(col("signup_date") > current_date(), "REVIEW")
        .otherwise("VALID"),
    )

    # 4. Derive signup date dimensions.
    customers_df = (
        customers_df
        .withColumn("signup_year", year(col("signup_date")))
        .withColumn("signup_month", month(col("signup_date")))
        .withColumn("signup_day", dayofmonth(col("signup_date")))
        .withColumn(
            "customer_tenure_days",
            when(
                col("signup_date").isNotNull()
                & (col("signup_date") <= current_date()),
                datediff(current_date(), col("signup_date")),
            ),
        )
    )

    # 5. Derive customer status.
    customers_df = customers_df.withColumn(
        "customer_status",
        when(col("customer_tenure_days").isNull(), "UNKNOWN")
        .when(col("customer_tenure_days") < 90, "NEW")
        .when(col("customer_tenure_days") <= 365, "ACTIVE")
        .otherwise("ESTABLISHED"),
    )

    # 6. Check duplicate customer IDs.
    # Do not arbitrarily drop duplicates: without a reliable update timestamp,
    # choosing which record to keep could discard legitimate information.
    duplicate_ids = (
        customers_df
        .filter(col("customer_id").isNotNull() & (col("customer_id") != ""))
        .groupBy("customer_id")
        .agg(count("*").alias("record_count"))
        .filter(col("record_count") > 1)
    )

    duplicate_id_count = duplicate_ids.count()

    if duplicate_id_count:
        logger.warning(
            "Found %s customer IDs with duplicate records; "
            "records are retained for review.",
            duplicate_id_count,
        )

        customers_df = customers_df.withColumn(
            "customer_data_quality_status",
            when(
                col("customer_id").isin(
                    [row["customer_id"] for row in duplicate_ids.select(
                        "customer_id"
                    ).limit(1000).collect()]
                ),
                "REVIEW",
            ).otherwise(col("customer_data_quality_status")),
        )

    # 7. Log quality counts.
    customers_df.groupBy(
        "customer_data_quality_status"
    ).count().show()

    # 8. Write refined Parquet data.
    (
        customers_df.write
        .mode("overwrite")
        .parquet(OUTPUT_PATH)
    )

    logger.info("Refined customers written to %s", OUTPUT_PATH)

    # 9. Verify output.
    refined_df = glue_context.spark_session.read.parquet(OUTPUT_PATH)
    output_count = refined_df.count()

    if output_count != source_count:
        logger.warning(
            "Customer row count changed: source=%s, refined=%s",
            source_count,
            output_count,
        )

    logger.info(
        "Customers transformation completed: %s rows",
        output_count,
    )

    return {
        "dataset": "customers",
        "source_count": source_count,
        "output_count": output_count,
        "output_path": OUTPUT_PATH,
        "status": "SUCCESS",
    }
