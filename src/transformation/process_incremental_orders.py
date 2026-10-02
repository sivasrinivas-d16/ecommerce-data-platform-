from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col,
    to_date,
    year,
    month,
    dayofmonth,
    round,
    when
)


# =========================================================
# Spark Session
# =========================================================

spark = (
    SparkSession.builder
    .appName("ProcessIncrementalOrders")
    .master("local[*]")
    .config("spark.driver.host", "127.0.0.1")
    .config("spark.driver.bindAddress", "127.0.0.1")
    .getOrCreate()
)

spark.sparkContext.setLogLevel("WARN")


# =========================================================
# Paths
# =========================================================

existing_orders_path = (
    r".\ecommerce-data-platform\data\curated\orders"
)

incremental_path = (
    r".\ecommerce-data-platform\data\raw\incremental\orders_batch_002"
)

output_path = (
    r".\ecommerce-data-platform\data\curated\incremental_orders"
)


# =========================================================
# Read Existing Curated Orders
# =========================================================

existing_orders = (
    spark.read
    .parquet(existing_orders_path)
)

existing_count = existing_orders.count()

print(f"Existing curated orders: {existing_count}")


# =========================================================
# Read Incremental Batch
# =========================================================

incremental_orders = (
    spark.read
    .parquet(incremental_path)
)

incremental_count = incremental_orders.count()

print(f"Incoming incremental orders: {incremental_count}")


# =========================================================
# Identify New Orders
# =========================================================

existing_order_ids = (
    existing_orders
    .select("order_id")
    .dropDuplicates()
)


# Read previously processed incremental orders

processed_incremental = (
    spark.read
    .parquet(output_path)
)


processed_incremental_ids = (
    processed_incremental
    .select("order_id")
    .dropDuplicates()
)


# Combine all already-processed order IDs

all_processed_order_ids = (
    existing_order_ids
    .union(processed_incremental_ids)
    .dropDuplicates()
)


# Keep only genuinely new orders

new_orders = (
    incremental_orders
    .join(
        all_processed_order_ids,
        on="order_id",
        how="left_anti"
    )
)


new_order_count = new_orders.count()

print(
    f"New orders to process: "
    f"{new_order_count}"
)


# =========================================================
# Transform New Orders
# =========================================================

processed_orders = (
    new_orders

    # Date attributes
    .withColumn(
        "order_date",
        to_date(col("order_timestamp"))
    )

    .withColumn(
        "order_year",
        year(col("order_timestamp"))
    )

    .withColumn(
        "order_month",
        month(col("order_timestamp"))
    )

    .withColumn(
        "order_day",
        dayofmonth(col("order_timestamp"))
    )

    # Recalculate order amount
    .withColumn(
        "calculated_order_amount",
        round(
            col("quantity") * col("unit_price"),
            2
        )
    )

    # Validate order amount
    .withColumn(
        "order_amount_valid",
        when(
            col("order_amount")
            == col("calculated_order_amount"),
            True
        ).otherwise(False)
    )

    # Completed order flag
    .withColumn(
        "is_completed_order",
        when(
            col("order_status") == "COMPLETED",
            True
        ).otherwise(False)
    )

    # Paid order flag
    .withColumn(
        "is_paid_order",
        when(
            col("payment_status") == "PAID",
            True
        ).otherwise(False)
    )

    # Net order amount
    .withColumn(
        "net_order_amount",
        when(
            col("order_status") == "CANCELLED",
            0
        ).otherwise(
            col("order_amount")
        )
    )
)


# =========================================================
# Validation
# =========================================================

processed_count = processed_orders.count()

invalid_amount_count = (
    processed_orders
    .filter(
        ~col("order_amount_valid")
    )
    .count()
)

duplicate_id_count = (
    processed_orders
    .groupBy("order_id")
    .count()
    .filter(
        col("count") > 1
    )
    .count()
)


print("\n--- Incremental Processing Validation ---")

print(
    f"Processed records      : "
    f"{processed_count}"
)

print(
    f"Invalid order amounts  : "
    f"{invalid_amount_count}"
)

print(
    f"Duplicate order IDs    : "
    f"{duplicate_id_count}"
)


# =========================================================
# Write Processed Incremental Orders
# =========================================================

if processed_count > 0:

    (
        processed_orders
        .write
        .mode("overwrite")
        .partitionBy(
            "order_year",
            "order_month"
        )
        .parquet(output_path)
    )

    print(
        "\nIncremental orders "
        "processed successfully."
    )

else:

    print(
        "\nNo new orders to process."
    )


# =========================================================
# Verify Output
# =========================================================

if processed_count > 0:

    saved_orders = (
        spark.read
        .parquet(output_path)
    )

    saved_count = saved_orders.count()

    print(
        f"Saved incremental records: "
        f"{saved_count}"
    )


# =========================================================
# Stop Spark
# =========================================================

spark.stop()