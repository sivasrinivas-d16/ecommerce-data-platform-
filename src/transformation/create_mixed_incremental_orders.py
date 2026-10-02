from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col,
    concat,
    lit,
    row_number,
    format_string
)
from pyspark.sql.window import Window


# =========================================================
# Spark Session
# =========================================================

spark = (
    SparkSession.builder
    .appName("CreateMixedIncrementalOrders")
    .master("local[*]")
    .config("spark.driver.host", "127.0.0.1")
    .config("spark.driver.bindAddress", "127.0.0.1")
    .getOrCreate()
)

spark.sparkContext.setLogLevel("WARN")


# =========================================================
# Paths
# =========================================================

source_path = (
    r".\ecommerce-data-platform\data\orders.csv"
)

output_path = (
    r".\ecommerce-data-platform\data\raw\incremental\orders_batch_002"
)


# =========================================================
# Read Existing Orders
# =========================================================

orders_df = (
    spark.read
    .option("header", True)
    .option("inferSchema", True)
    .csv(source_path)
)

original_count = orders_df.count()

print(f"Original orders: {original_count}")


# =========================================================
# 45,000 Existing Orders
# =========================================================

existing_batch = (
    orders_df
    .orderBy(
        col("order_timestamp").desc()
    )
    .limit(45000)
)


# =========================================================
# Create 5,000 New Orders
# =========================================================

window_spec = Window.orderBy(
    col("order_timestamp").desc()
)

new_orders = (
    orders_df
    .orderBy(
        col("order_timestamp").desc()
    )
    .limit(5000)

    # Generate valid O######## IDs
    .withColumn(
        "order_id",
        format_string(
            "O%08d",
            row_number().over(window_spec) + 1000000
        )
    )
)


# =========================================================
# Combine Existing + New Orders
# =========================================================

mixed_batch = (
    existing_batch
    .unionByName(new_orders)
)


mixed_count = mixed_batch.count()

print(f"Mixed batch count: {mixed_count}")

print("\nExisting batch:")
print(existing_batch.count())

print("\nNew batch:")
print(new_orders.count())


# =========================================================
# Write Incremental Batch
# =========================================================

(
    mixed_batch
    .write
    .mode("overwrite")
    .parquet(output_path)
)


print(
    "\nMixed incremental batch "
    "created successfully."
)


# =========================================================
# Verify Saved Batch
# =========================================================

saved_batch = (
    spark.read
    .parquet(output_path)
)

saved_count = saved_batch.count()

print(
    f"Saved incremental rows: "
    f"{saved_count}"
)


print("\nSample records:")

(
    saved_batch
    .select(
        "order_id",
        "customer_id",
        "product_id",
        "order_timestamp"
    )
    .show(
        10,
        truncate=False
    )
)


# =========================================================
# Duplicate Detection
# =========================================================

existing_orders = (
    orders_df
    .select("order_id")
    .dropDuplicates()
)


incremental_orders = (
    spark.read
    .parquet(output_path)
)


duplicate_orders = (
    incremental_orders
    .join(
        existing_orders,
        on="order_id",
        how="inner"
    )
)


new_orders_detected = (
    incremental_orders
    .join(
        existing_orders,
        on="order_id",
        how="left_anti"
    )
)


print("\n--- Incremental Batch Validation ---")

print(
    f"Incremental records : "
    f"{incremental_orders.count()}"
)

print(
    f"Duplicate orders     : "
    f"{duplicate_orders.count()}"
)

print(
    f"New orders           : "
    f"{new_orders_detected.count()}"
)


# =========================================================
# Stop Spark
# =========================================================

spark.stop()