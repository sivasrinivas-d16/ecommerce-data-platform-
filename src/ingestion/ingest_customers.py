
from pyspark.sql.types import (
    StructType,
    StructField,
    StringType
)
from pyspark.sql.functions import (
    col,
    to_date,
    trim,
    when,
    count,
    sum as spark_sum
)


def process_customers(
    spark,
    input_path,
    output_path,
    write_mode
):
    """
    Ingest customer CSV data into the raw Parquet layer.

    Preserves all input rows while normalizing whitespace
    and converting signup_date to DateType.
    """

    # 1. Define the source schema
    customer_schema = StructType([
        StructField("customer_id", StringType(), True),
        StructField("name", StringType(), True),
        StructField("email", StringType(), True),
        StructField("city", StringType(), True),
        StructField("state", StringType(), True),
        StructField("country", StringType(), True),
        StructField("signup_date", StringType(), True)
    ])

    # 2. Read source CSV files
    customers_df = (
        spark.read
        .option("header", True)
        .option("mode", "PERMISSIVE")
        .schema(customer_schema)
        .csv(input_path)
    )

    source_count = customers_df.count()
    print(f"Customers source row count: {source_count}")
    customers_df.printSchema()

    # 3. Normalize whitespace in string columns
    for column_name in customers_df.columns:
        customers_df = customers_df.withColumn(
            column_name,
            trim(col(column_name))
        )

    # 4. Convert signup_date using the expected source format
    customers_df = customers_df.withColumn(
        "signup_date",
        to_date(col("signup_date"), "dd-MM-yyyy")
    )

    # 5. Report potential data-quality issues
    quality_summary = customers_df.agg(
        spark_sum(
            when(
                col("customer_id").isNull()
                | (col("customer_id") == ""),
                1
            ).otherwise(0)
        ).alias("missing_customer_id"),
        spark_sum(
            when(col("signup_date").isNull(), 1)
            .otherwise(0)
        ).alias("missing_or_invalid_signup_date")
    ).first()

    print("Missing customer IDs:", quality_summary["missing_customer_id"])
    print(
        "Missing or invalid signup dates:",
        quality_summary["missing_or_invalid_signup_date"]
    )

    # 6. Write to the raw layer
    (
        customers_df.write
        .mode(write_mode)
        .format("parquet")
        .save(output_path)
    )

    print(f"Customers written to: {output_path}")

    # 7. Verify raw output
    raw_customers_df = spark.read.parquet(output_path)
    raw_count = raw_customers_df.count()

    print(f"Source rows: {source_count}")
    print(f"Raw rows:    {raw_count}")

    if source_count != raw_count:
        raise RuntimeError(
            "Customer row-count mismatch: "
            f"source={source_count}, raw={raw_count}"
        )

    raw_customers_df.printSchema()
    raw_customers_df.show(5, truncate=False)

    print("Customer raw ingestion and row-count verification completed.")
