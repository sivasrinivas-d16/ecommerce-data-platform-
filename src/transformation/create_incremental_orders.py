import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import col


spark = (
    SparkSession.builder
    .appName("CreateIncrementalOrders")
    .master("local[*]")
    .config("spark.driver.host", "127.0.0.1")
    .config("spark.driver.bindAddress", "127.0.0.1")
    .getOrCreate()
)

spark.sparkContext.setLogLevel("WARN")


# ---------------------------------------------------------
# Paths
# ---------------------------------------------------------

source_path = (
    r".\ecommerce-data-platform\data\orders.csv"
)

output_path = (
    r".\ecommerce-data-platform\data\raw\incremental\orders_batch_001"
)


# ---------------------------------------------------------
# Read existing orders
# ---------------------------------------------------------

orders_df = (
    spark.read
    .option("header", True)
    .option("inferSchema", True)
    .csv(source_path)
)


print(f"Original order count: {orders_df.count()}")


# ---------------------------------------------------------
# Select 50,000 orders as incremental test batch
# ---------------------------------------------------------

incremental_orders = (
    orders_df
    .orderBy(col("order_timestamp").desc())
    .limit(50000)
)


print(f"Incremental batch count: {incremental_orders.count()}")


# ---------------------------------------------------------
# Write incremental batch
# ---------------------------------------------------------

(
    incremental_orders
    .write
    .mode("overwrite")
    .parquet(output_path)
)


print("\nIncremental batch created successfully.")

saved_batch = spark.read.parquet(output_path)

print(f"Saved incremental rows: {saved_batch.count()}")

saved_batch.select(
    "order_id",
    "customer_id",
    "product_id",
    "order_timestamp"
).show(10, truncate=False)

existing_orders = (
    spark.read
    .option("header", True)
    .option("inferSchema", True)
    .csv(source_path)
    .select("order_id")
)

incremental_orders = (
    spark.read
    .parquet(output_path)
    .select("order_id")
)

duplicate_orders = (
    incremental_orders
    .join(existing_orders, on="order_id", how="inner")
)

duplicate_count = duplicate_orders.count()

print(f"Incremental records : {incremental_orders.count()}")
print(f"Duplicate orders     : {duplicate_count}")

new_orders = (
    incremental_orders
    .join(
        existing_orders,
        on="order_id",
        how="left_anti"
    )
)

new_order_count = new_orders.count()

print(f"New orders to process : {new_order_count}")

spark.stop()