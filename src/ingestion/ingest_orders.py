
from pyspark.sql.types import (
    StructType,
    StructField,
    StringType,
    IntegerType,
    DecimalType,
    TimestampType
)


# ============================================================
# 1. Define Orders Schema
# ============================================================

orders_schema = StructType([
    StructField("order_id", StringType(), False),
    StructField("customer_id", StringType(), False),
    StructField("product_id", StringType(), False),
    StructField("quantity", IntegerType(), False),
    StructField("unit_price", DecimalType(12, 2), False),
    StructField("order_amount", DecimalType(14, 2), False),
    StructField("order_status", StringType(), False),
    StructField("payment_status", StringType(), False),
    StructField("order_timestamp", TimestampType(), False)
])


# ============================================================
# 2. Reusable Orders Ingestion Function
# ============================================================

def process_orders(
    spark,
    input_path,
    output_path,
    write_mode
):
    """
    Read Orders CSV, perform ingestion checks,
    and write the dataset to the S3 raw layer.
    """

    # --------------------------------------------------------
    # 3. Read Orders CSV
    # --------------------------------------------------------

    orders_df = (
        spark.read
        .option("header", True)
        .schema(orders_schema)
        .csv(input_path)
    )

    print("Orders DataFrame loaded successfully")
    print("Input path:", input_path)

    # --------------------------------------------------------
    # 4. Record Count
    # --------------------------------------------------------

    row_count = orders_df.count()
    print("Orders row count:", row_count)

    # --------------------------------------------------------
    # 5. Schema and Sample Records
    # --------------------------------------------------------

    print("Orders Schema:")
    orders_df.printSchema()

    print("Sample Orders:")
    orders_df.show(5, truncate=False)

    # --------------------------------------------------------
    # 6. Basic Ingestion Validation
    # --------------------------------------------------------

    print("Orders ingestion validation")

    null_order_ids = (
        orders_df.filter(
            orders_df.order_id.isNull()
        ).count()
    )

    null_customer_ids = (
        orders_df.filter(
            orders_df.customer_id.isNull()
        ).count()
    )

    null_product_ids = (
        orders_df.filter(
            orders_df.product_id.isNull()
        ).count()
    )

    duplicate_order_ids = (
        orders_df.groupBy("order_id")
        .count()
        .filter("count > 1")
        .count()
    )

    print("Null order IDs:", null_order_ids)
    print("Null customer IDs:", null_customer_ids)
    print("Null product IDs:", null_product_ids)
    print("Duplicate order IDs:", duplicate_order_ids)

    # --------------------------------------------------------
    # 7. Write to Raw Layer
    # --------------------------------------------------------

    print("Starting Orders raw-layer write")
    print("Output path:", output_path)

    (
        orders_df.write
        .mode(write_mode)
        .format("parquet")
        .save(output_path)
    )

    print("Orders raw-layer write completed")

    # --------------------------------------------------------
    # 8. Verify Raw Layer
    # --------------------------------------------------------

    raw_orders_df = spark.read.parquet(output_path)

    raw_count = raw_orders_df.count()

    print("Raw Orders row count:", raw_count)

    print("Raw Orders schema:")
    raw_orders_df.printSchema()

    print("Raw Orders sample:")
    raw_orders_df.show(5, truncate=False)

    if raw_count != row_count:
        raise RuntimeError(
            "Orders row-count verification failed: "
            f"source={row_count}, raw={raw_count}"
        )

    print("Orders raw ingestion completed successfully")
