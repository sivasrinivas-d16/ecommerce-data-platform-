
from pyspark.sql.types import (
    StructType,
    StructField,
    StringType,
    IntegerType,
    DecimalType,
    DateType
)


# ============================================================
# 1. Product Schema
# ============================================================

product_schema = StructType([
    StructField("product_id", StringType(), True),
    StructField("product_name", StringType(), True),
    StructField("category", StringType(), True),
    StructField("subcategory", StringType(), True),
    StructField("brand", StringType(), True),
    StructField("price", DecimalType(12, 2), True),
    StructField("stock_quantity", IntegerType(), True),
    StructField("product_status", StringType(), True),
    StructField("created_date", DateType(), True)
])


# ============================================================
# 2. Reusable Products Ingestion Function
# ============================================================

def process_products(
    spark,
    input_path,
    output_path,
    write_mode
):
    """
    Read Products CSV and write it to the raw layer.
    Spark and Glue are initialized by the main script.
    """

    # --------------------------------------------------------
    # 3. Read Products CSV
    # --------------------------------------------------------

    print("Reading Products source:")
    print(input_path)

    products_df = (
        spark.read
        .option("header", True)
        .schema(product_schema)
        .csv(input_path)
    )

    # --------------------------------------------------------
    # 4. Basic Verification
    # --------------------------------------------------------

    record_count = products_df.count()

    print(f"Products record count: {record_count}")

    if record_count == 0:
        raise RuntimeError(
            "Products ingestion failed: source contains zero records."
        )

    print("Products schema:")
    products_df.printSchema()

    products_df.show(5, truncate=False)

    # --------------------------------------------------------
    # 5. Write Raw Parquet
    # --------------------------------------------------------

    print("Writing Products raw Parquet:")
    print(output_path)

    (
        products_df.write
        .mode(write_mode)
        .format("parquet")
        .save(output_path)
    )

    # --------------------------------------------------------
    # 6. Verify Output
    # --------------------------------------------------------

    output_df = spark.read.parquet(output_path)
    output_count = output_df.count()

    print(f"Raw Products output count: {output_count}")

    if record_count != output_count:
        raise RuntimeError(
            "Products ingestion count mismatch: "
            f"input={record_count}, output={output_count}"
        )

    print("Products raw ingestion completed successfully.")
