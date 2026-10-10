
from pyspark.sql.types import (
    StructType,
    StructField,
    StringType,
    DecimalType,
    TimestampType
)


# ============================================================
# 1. Define Payment Schema
# ============================================================

payment_schema = StructType([
    StructField("payment_id", StringType(), True),
    StructField("order_id", StringType(), True),
    StructField("customer_id", StringType(), True),
    StructField("payment_method", StringType(), True),
    StructField("payment_status", StringType(), True),
    StructField("payment_amount", DecimalType(12, 2), True),
    StructField("transaction_reference", StringType(), True),
    StructField("payment_timestamp", TimestampType(), True)
])


# ============================================================
# 2. Reusable Payments Ingestion Function
# ============================================================

def process_payments(
    spark,
    input_path,
    output_path,
    write_mode
):
    """
    Read Payments CSV and write it to the S3 raw layer.

    Parameters:
        spark: Spark session managed by AWS Glue
        input_path: Source CSV S3 path
        output_path: Raw output S3 path
        write_mode: Output write mode
    """

    # --------------------------------------------------------
    # 3. Read Payment Data
    # --------------------------------------------------------

    payments_df = (
        spark.read
        .option("header", True)
        .schema(payment_schema)
        .csv(input_path)
    )

    print("Payments loaded successfully")
    print("Input path:", input_path)

    row_count = payments_df.count()
    print("Payments row count:", row_count)

    # --------------------------------------------------------
    # 4. Display Schema and Sample
    # --------------------------------------------------------

    print("Payment Schema:")
    payments_df.printSchema()

    print("Payment Sample:")
    payments_df.show(5, truncate=False)

    # --------------------------------------------------------
    # 5. Write to Raw Layer
    # --------------------------------------------------------

    (
        payments_df.write
        .mode(write_mode)
        .format("parquet")
        .save(output_path)
    )

    print("Payments written successfully to raw layer")
    print("Output path:", output_path)

    # --------------------------------------------------------
    # 6. Verify Raw Layer
    # --------------------------------------------------------

    raw_payments_df = spark.read.parquet(output_path)

    raw_count = raw_payments_df.count()

    print("Raw payment row count:", raw_count)

    print("Raw Payment Schema:")
    raw_payments_df.printSchema()

    print("Raw Payment Sample:")
    raw_payments_df.show(5, truncate=False)

    if raw_count != row_count:
        raise RuntimeError(
            "Payments row-count verification failed: "
            f"source={row_count}, raw={raw_count}"
        )

    print("Payments raw ingestion completed successfully")
