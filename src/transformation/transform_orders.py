from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    to_date,
    year,
    month,
    dayofmonth,
    col,
    round,
    when
)

# ============================================================
# 1. Create Spark Session
# ============================================================

spark = (
    SparkSession.builder
    .appName("ECommerceOrderTransformation")
    .master("local[*]")
    .config("spark.driver.host", "127.0.0.1")
    .config("spark.driver.bindAddress", "127.0.0.1")
    .config("spark.hadoop.fs.permissions.umask-mode", "000")
    .getOrCreate()
)

print("Spark version:", spark.version)
print("Order transformation job started")

# ============================================================
# 2. Read Orders from Raw Layer
# ============================================================

raw_orders_path = r".\ecommerce-data-platform\data\raw\orders"

orders_df = spark.read.parquet(raw_orders_path)

print("\nRaw Orders loaded successfully")

# ============================================================
# 3. Inspect Raw Orders
# ============================================================

print("Raw Orders row count:", orders_df.count())

print("\nRaw Orders Schema:")
orders_df.printSchema()

print("\nSample Raw Orders:")
orders_df.show(5, truncate=False)

# ============================================================
# 4. Create Order Date Dimensions
# ============================================================

transformed_orders_df = (
    orders_df
    .withColumn("order_date", to_date("order_timestamp"))
    .withColumn("order_year", year("order_timestamp"))
    .withColumn("order_month", month("order_timestamp"))
    .withColumn("order_day", dayofmonth("order_timestamp"))
)

print("\nDate transformation completed")

print("\nTransformed Orders:")
transformed_orders_df.select(
    "order_id",
    "order_timestamp",
    "order_date",
    "order_year",
    "order_month",
    "order_day"
).show(10, truncate=False)

# ============================================================
# 5. Validate Order Amount
# ============================================================

transformed_orders_df = (
    transformed_orders_df
    .withColumn(
        "calculated_order_amount",
        round(col("quantity") * col("unit_price"), 2)
    )
    .withColumn(
        "order_amount_valid",
        col("order_amount") == col("calculated_order_amount")
    )
)

print("\nOrder amount validation completed")

transformed_orders_df.select(
    "order_id",
    "quantity",
    "unit_price",
    "order_amount",
    "calculated_order_amount",
    "order_amount_valid"
).show(10, truncate=False)

print(
    "Invalid order amounts:",
    transformed_orders_df
    .filter(~col("order_amount_valid"))
    .count()
)

# ============================================================
# 6. Create Business Status Flags
# ============================================================

transformed_orders_df = (
    transformed_orders_df
    .withColumn(
        "is_completed_order",
        col("order_status") == "COMPLETED"
    )
    .withColumn(
        "is_paid_order",
        col("payment_status") == "PAID"
    )
)

print("\nBusiness status flags created")

transformed_orders_df.select(
    "order_id",
    "order_status",
    "payment_status",
    "is_completed_order",
    "is_paid_order"
).show(10, truncate=False)

print("\nOrder Status Distribution:")
transformed_orders_df.groupBy(
    "order_status"
).count().orderBy(
    col("count").desc()
).show()

print("\nPayment Status Distribution:")
transformed_orders_df.groupBy(
    "payment_status"
).count().orderBy(
    col("count").desc()
).show()

# ============================================================
# 7. Calculate Net Order Amount
# ============================================================

transformed_orders_df = (
    transformed_orders_df
    .withColumn(
        "net_order_amount",
        when(
            col("is_completed_order"),
            col("order_amount")
        ).otherwise(0)
    )
)

print("\nNet order amount calculated")

transformed_orders_df.select(
    "order_id",
    "order_status",
    "order_amount",
    "is_completed_order",
    "net_order_amount"
).show(10, truncate=False)

print(
    "Total Net Revenue:",
    transformed_orders_df
    .agg({"net_order_amount": "sum"})
    .collect()[0][0]
)

# ---------------------------------------------------------
# Step 9: Write Transformed Data to Curated Layer
# ---------------------------------------------------------

curated_orders_path = r".\ecommerce-data-platform\data\curated\orders"

(
    transformed_orders_df
    .write
    .mode("overwrite")
    .partitionBy("order_year", "order_month")
    .parquet(curated_orders_path)
)

print("Curated orders data written successfully.")

# ---------------------------------------------------------
# Step 10: Verify Curated Layer
# ---------------------------------------------------------

curated_orders_df = spark.read.parquet(curated_orders_path)

print("Curated row count:", curated_orders_df.count())

curated_orders_df.printSchema()

curated_orders_df.show(5, truncate=False)

# ---------------------------------------------------------
# Step 11: Test Partition Pruning
# ---------------------------------------------------------

filtered_orders_df = (
    curated_orders_df
    .filter(
        (col("order_year") == 2025) &
        (col("order_month") == 6)
    )
)

print("Filtered order count:", filtered_orders_df.count())

print("Execution Plan:")
filtered_orders_df.explain(True)


# ============================================================
# 12. Stop Spark
# ============================================================

spark.stop()

print("\nTransformation job completed successfully")

