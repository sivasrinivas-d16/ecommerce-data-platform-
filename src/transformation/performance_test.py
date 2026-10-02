from pyspark.sql import SparkSession
from pyspark.sql.functions import col, sum
from pyspark.sql.functions import broadcast
from pyspark import StorageLevel


spark = (
    SparkSession.builder
    .appName("SparkPerformanceTest")
    .master("local[*]")
    .config("spark.driver.host", "127.0.0.1")
    .config("spark.driver.bindAddress", "127.0.0.1")
    .getOrCreate()
)

spark.sparkContext.setLogLevel("WARN")


orders_path = (
    r".\ecommerce-data-platform\data\curated\orders"
)

orders_df = spark.read.parquet(orders_path)

print(f"Orders: {orders_df.count()}")


# =========================================================
# Partition Pruning Test
# =========================================================

filtered_orders = (
    orders_df
    .filter(
        (col("order_year") == 2025)
        & (col("order_month") == 6)
    )
)

print("\n--- Filtered Records ---")

print(
    f"Records for 2025-06: "
    f"{filtered_orders.count()}"
)


print("\n--- Partition Pruning Plan ---")

filtered_orders.explain(
    mode="formatted"
)

# =========================================================
# Repartition vs Coalesce
# =========================================================

print("\n--- Current Partitions ---")

print(
    f"Current partitions: "
    f"{orders_df.rdd.getNumPartitions()}"
)


# ---------------------------------------------------------
# Repartition
# ---------------------------------------------------------

repartitioned_df = (
    orders_df
    .repartition(10)
)

print("\n--- Repartition ---")

print(
    f"Repartitioned partitions: "
    f"{repartitioned_df.rdd.getNumPartitions()}"
)


# ---------------------------------------------------------
# Coalesce
# ---------------------------------------------------------

coalesced_df = (
    orders_df
    .coalesce(10)
)

print("\n--- Coalesce ---")

print(
    f"Coalesced partitions: "
    f"{coalesced_df.rdd.getNumPartitions()}"
)


# =========================================================
# Broadcast Join Test
# =========================================================

customers_path = (
    r".\ecommerce-data-platform\data\curated\customers"
)

customers_df = (
    spark.read
    .parquet(customers_path)
)


print("\n--- Dataset Sizes ---")

print(
    f"Orders    : {orders_df.count()}"
)

print(
    f"Customers : {customers_df.count()}"
)


# =========================================================
# Normal Join
# =========================================================

normal_join = (
    orders_df
    .join(
        customers_df.select(
            "customer_id",
            "customer_status"
        ),
        on="customer_id",
        how="left"
    )
)


print("\n--- Normal Join Plan ---")

normal_join.explain(
    mode="formatted"
)


# =========================================================
# Broadcast Join
# =========================================================

broadcast_join = (
    orders_df
    .join(
        broadcast(
            customers_df.select(
                "customer_id",
                "customer_status"
            )
        ),
        on="customer_id",
        how="left"
    )
)


print("\n--- Broadcast Join Plan ---")

broadcast_join.explain(
    mode="formatted"
)

spark.conf.set(
    "spark.sql.autoBroadcastJoinThreshold",
    -1
)

normal_join = (
    orders_df
    .join(
        customers_df.select(
            "customer_id",
            "customer_status"
        ),
        on="customer_id",
        how="left"
    )
)

print("\n--- Normal Join Without Auto Broadcast ---")

normal_join.explain(
    mode="formatted"
)

spark.conf.set(
    "spark.sql.autoBroadcastJoinThreshold",
    -1
)

normal_join = (
    orders_df.join(
        customers_df.select(
            "customer_id",
            "customer_status"
        ),
        on="customer_id",
        how="left"
    )
)

normal_join.explain(mode="formatted")

# ============================================================
# TEST 6 — CACHING / PERSISTENCE
# ============================================================

cached_orders = orders_df.filter(
    col("order_status") == "COMPLETED"
)

print("\n--- Caching Test ---")

print("Before cache:")
cached_orders.explain(mode="formatted")

cached_orders.cache()

print("\nFirst action:")
print("Record count:", cached_orders.count())

print("\nStorage level after cache:")
print(cached_orders.storageLevel)

print("\nSecond action:")
print("Record count:", cached_orders.count())

cached_orders.unpersist()

spark.stop()