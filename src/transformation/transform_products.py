
import logging

from pyspark.sql import DataFrame
from pyspark.sql.functions import col, trim, upper, when, count

DATABASE = "ecommerce_data_platform"
TABLE = "products"
OUTPUT_PATH = (
    "s3://ecommerce-data-platform-version1/refined/products/"
)

logger = logging.getLogger("RefinedProducts")

REQUIRED_COLUMNS = [
    "product_id",
    "product_name",
    "category",
    "subcategory",
    "brand",
    "price",
    "stock_quantity",
    "product_status",
]


def validate_columns(df: DataFrame) -> None:
    missing = sorted(set(REQUIRED_COLUMNS) - set(df.columns))
    if missing:
        raise ValueError(
            f"Products table is missing required columns: {missing}"
        )


def run_transformation(glue_context) -> dict:
    logger.info("Starting products refined transformation")

    source_dyf = glue_context.create_dynamic_frame.from_catalog(
        database=DATABASE,
        table_name=TABLE,
    )
    products_df = source_dyf.toDF()

    validate_columns(products_df)

    source_count = products_df.count()
    logger.info("Raw product rows: %s", source_count)

    # Standardize text fields.
    products_df = (
        products_df
        .withColumn("product_id", trim(col("product_id").cast("string")))
        .withColumn("product_name", trim(col("product_name")))
        .withColumn("category", trim(col("category")))
        .withColumn("subcategory", trim(col("subcategory")))
        .withColumn("brand", trim(col("brand")))
        .withColumn(
            "product_status",
            upper(trim(col("product_status"))),
        )
    )

    # Ensure numeric fields use suitable types.
    products_df = (
        products_df
        .withColumn("price", col("price").cast("decimal(18,2)"))
        .withColumn("stock_quantity", col("stock_quantity").cast("long"))
    )

    # Flag invalid data instead of silently removing records.
    products_df = products_df.withColumn(
        "product_data_quality_status",
        when(
            col("product_id").isNull()
            | (col("product_id") == "")
            | col("product_name").isNull()
            | (col("product_name") == "")
            | col("price").isNull()
            | (col("price") < 0)
            | col("stock_quantity").isNull()
            | (col("stock_quantity") < 0),
            "REVIEW",
        ).otherwise("VALID"),
    )

    # Preserve existing price thresholds.
    products_df = products_df.withColumn(
        "price_category",
        when(col("price").isNull(), "UNKNOWN")
        .when(col("price") < 1000, "LOW")
        .when(col("price") <= 10000, "MEDIUM")
        .when(col("price") <= 50000, "HIGH")
        .otherwise("PREMIUM"),
    )

    # Preserve stock thresholds, with safeguards for invalid values.
    products_df = products_df.withColumn(
        "stock_status",
        when(col("stock_quantity").isNull(), "UNKNOWN")
        .when(col("stock_quantity") < 0, "INVALID")
        .when(col("stock_quantity") == 0, "OUT_OF_STOCK")
        .when(col("stock_quantity") <= 100, "LOW_STOCK")
        .when(col("stock_quantity") <= 1000, "MEDIUM_STOCK")
        .otherwise("IN_STOCK"),
    )

    # Detect duplicate product IDs. Retain records for review because
    # no reliable record-version field has been specified.
    duplicate_ids = (
        products_df
        .filter(col("product_id").isNotNull() & (col("product_id") != ""))
        .groupBy("product_id")
        .agg(count("*").alias("record_count"))
        .filter(col("record_count") > 1)
        .select("product_id")
    )

    duplicate_id_count = duplicate_ids.count()

    if duplicate_id_count:
        logger.warning(
            "Found %s product IDs with duplicate records; "
            "records are retained for review.",
            duplicate_id_count,
        )

        products_df = products_df.join(
            duplicate_ids.withColumn("_duplicate_product_id", col("product_id"))
            .drop("product_id"),
            products_df["product_id"] == col("_duplicate_product_id"),
            "left",
        ).withColumn(
            "product_data_quality_status",
            when(
                col("_duplicate_product_id").isNotNull(),
                "REVIEW",
            ).otherwise(col("product_data_quality_status")),
        ).drop("_duplicate_product_id")

    products_df.groupBy("product_data_quality_status").count().show()
    products_df.groupBy("price_category").count().show()
    products_df.groupBy("stock_status").count().show()

    # Write refined Parquet data.
    products_df.write.mode("overwrite").parquet(OUTPUT_PATH)

    logger.info("Refined products written to %s", OUTPUT_PATH)

    # Verify output.
    refined_df = products_df.sparkSession.read.parquet(OUTPUT_PATH)
    output_count = refined_df.count()

    if output_count != source_count:
        logger.warning(
            "Product row count changed: source=%s, refined=%s",
            source_count,
            output_count,
        )

    logger.info(
        "Products transformation completed: %s rows",
        output_count,
    )

    return {
        "dataset": "products",
        "source_count": source_count,
        "output_count": output_count,
        "output_path": OUTPUT_PATH,
        "status": "SUCCESS",
    }
