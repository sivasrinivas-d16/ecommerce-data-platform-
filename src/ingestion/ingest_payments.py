
from pyspark.sql.types import StructType, StructField, StringType, DecimalType
from pyspark.sql.functions import (
    col,
    trim,
    to_timestamp,
    when,
    sum as spark_sum
)


def process_payments(
    spark,
    input_path,
    output_path,
    write_mode
):
    """
    Read payment CSV data, convert types, report data-quality
    issues, and write the results to the S3 raw Parquet layer.
    """

    # 1. Read CSV columns as strings to control type conversion.
    source_schema = StructType([
        StructField("payment_id", StringType(), True),
        StructField("order_id", StringType(), True),
        StructField("customer_id", StringType(), True),
        StructField("payment_method", StringType(), True),
        StructField("payment_status", StringType(), True),
        StructField("payment_amount", StringType(), True),
        StructField("transaction_reference", StringType(), True),
        StructField("payment_timestamp", StringType(), True)
    ])

    payments_df = (
        spark.read
        .option("header", True)
        .option("mode", "PERMISSIVE")
        .schema(source_schema)
        .csv(input_path)
    )

    source_count = payments_df.count()
    print(f"Payments source row count: {source_count}")
    print(f"Input path: {input_path}")

    payments_df.printSchema()
    payments_df.show(5, truncate=False)

    # 2. Trim whitespace from source strings.
    for column_name in payments_df.columns:
        payments_df = payments_df.withColumn(
            column_name,
            trim(col(column_name))
        )

    # 3. Explicitly convert decimal and timestamp columns.
    payments_df = (
        payments_df
        .withColumn(
            "payment_amount",
            col("payment_amount").cast(DecimalType(12, 2))
        )
        .withColumn(
            "payment_timestamp",
            to_timestamp(
                col("payment_timestamp"),
                "yyyy-MM-dd HH:mm:ss"
            )
        )
    )

    # 4. Report missing required IDs and conversion failures.
    quality_summary = payments_df.agg(
        *[
            spark_sum(
                when(col(c).isNull(), 1).otherwise(0)
            ).alias(c)
            for c in [
                "payment_id",
                "order_id",
                "customer_id",
                "payment_amount",
                "payment_timestamp"
            ]
        ]
    ).first()

    print("Missing or invalid payment fields:")
    for field in [
        "payment_id",
        "order_id",
        "customer_id",
        "payment_amount",
        "payment_timestamp"
    ]:
        print(f"{field}: {quality_summary[field]}")

    # 5. Write to the raw Parquet layer.
    (
        payments_df.write
        .mode(write_mode)
        .format("parquet")
        .save(output_path)
    )

    print(f"Payments written to: {output_path}")

    # 6. Verify the output.
    raw_payments_df = spark.read.parquet(output_path)
    raw_count = raw_payments_df.count()

    print(f"Source rows: {source_count}")
    print(f"Raw rows:    {raw_count}")

    if raw_count != source_count:
        raise RuntimeError(
            f"Payment row-count mismatch: "
            f"source={source_count}, raw={raw_count}"
        )

    raw_payments_df.printSchema()
    raw_payments_df.show(5, truncate=False)

    print("Payments raw ingestion completed successfully.")
