
from pyspark.sql.types import (
    StructType,
    StructField,
    StringType,
    IntegerType,
    DecimalType
)
from pyspark.sql.functions import (
    col,
    trim,
    to_date,
    when,
    sum as spark_sum
)


def process_products(
    spark,
    input_path,
    output_path,
    write_mode
):
    """
    Read Products CSV, convert types, report data-quality
    issues, and write the data to the S3 raw Parquet layer.
    """

    # 1. Read source fields as strings for explicit conversion.
    source_schema = StructType([
        StructField("product_id", StringType(), True),
        StructField("product_name", StringType(), True),
        StructField("category", StringType(), True),
        StructField("subcategory", StringType(), True),
        StructField("brand", StringType(), True),
        StructField("price", StringType(), True),
        StructField("stock_quantity", StringType(), True),
        StructField("product_status", StringType(), True),
        StructField("created_date", StringType(), True)
    ])

    products_df = (
        spark.read
        .option("header", True)
        .option("mode", "PERMISSIVE")
        .schema(source_schema)
        .csv(input_path)
    )

    source_count = products_df.count()
    print(f"Products source row count: {source_count}")
    print(f"Input path: {input_path}")

    products_df.printSchema()
    products_df.show(5, truncate=False)

    # 2. Trim whitespace in string columns.
    for column_name in products_df.columns:
        products_df = products_df.withColumn(
            column_name,
            trim(col(column_name))
        )

    # 3. Explicitly convert numeric and date fields.
    products_df = (
        products_df
        .withColumn(
            "price",
            col("price").cast(DecimalType(12, 2))
        )
        .withColumn(
            "stock_quantity",
            col("stock_quantity").cast(IntegerType())
        )
        .withColumn(
            "created_date",
            to_date(col("created_date"), "yyyy-MM-dd")
        )
    )

    # 4. Report missing or invalid fields.
    fields_to_check = [
        "product_id",
        "price",
        "stock_quantity",
        "created_date"
    ]

    quality_summary = products_df.agg(*[
        spark_sum(
            when(col(field).isNull(), 1).otherwise(0)
        ).alias(field)
        for field in fields_to_check
    ]).first()

    print("Missing or invalid product fields:")
    for field in fields_to_check:
        print(f"{field}: {quality_summary[field]}")

    # 5. Write to the raw Parquet layer.
    (
        products_df.write
        .mode(write_mode)
        .format("parquet")
        .save(output_path)
    )

    print(f"Products written to: {output_path}")

    # 6. Verify raw output.
    output_df = spark.read.parquet(output_path)
    output_count = output_df.count()

    print(f"Source rows: {source_count}")
    print(f"Raw rows:    {output_count}")

    if source_count != output_count:
        raise RuntimeError(
            f"Products row-count mismatch: "
            f"source={source_count}, raw={output_count}"
        )

    print("Raw Products schema:")
    output_df.printSchema()
    output_df.show(5, truncate=False)

    print("Products raw ingestion completed successfully.")
