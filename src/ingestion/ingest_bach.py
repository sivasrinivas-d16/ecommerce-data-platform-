from pyspark.sql import SparkSession

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
# 2. Create Spark Session
# ============================================================

spark = (
    SparkSession.builder
    .appName("ECommerceBatchIngestion")
    .master("local[*]")
    .config("spark.driver.host", "127.0.0.1")
    .config("spark.driver.bindAddress", "127.0.0.1")
    .config("spark.hadoop.fs.permissions.umask-mode", "000")
    .getOrCreate()
)

print("Spark version:", spark.version)
print("Batch ingestion job started")


# ============================================================
# 3. Define Source Path
# ============================================================

orders_path = r".\ecommerce-data-platform\data\orders.csv"


# ============================================================
# 4. Read Orders CSV
# ============================================================

orders_df = (
    spark.read
    .option("header", True)
    .schema(orders_schema)
    .csv(orders_path)
)

print("\nOrders DataFrame loaded successfully")


# ============================================================
# 5. Display Record Count
# ============================================================

row_count = orders_df.count()

print("Orders row count:", row_count)


# ============================================================
# 6. Display Schema
# ============================================================

print("\nOrders Schema:")
orders_df.printSchema()


# ============================================================
# 7. Display Sample Records
# ============================================================

print("\nSample Orders:")
orders_df.show(5, truncate=False)

# ============================================================
# 8. Basic Ingestion Validation
# ============================================================

print("\nIngestion Validation")

print(
    "Null order IDs:",
    orders_df.filter(orders_df.order_id.isNull()).count()
)

print(
    "Null customer IDs:",
    orders_df.filter(orders_df.customer_id.isNull()).count()
)

print(
    "Null product IDs:",
    orders_df.filter(orders_df.product_id.isNull()).count()
)

print(
    "Duplicate order IDs:",
    orders_df.groupBy("order_id")
    .count()
    .filter("count > 1")
    .count()
)

# ============================================================
# 9. Write to Raw Layer
# ============================================================

raw_orders_path = r".\ecommerce-data-platform\data\raw\orders"

print("\nStarting Raw Layer write...")
print("Rows to write:", orders_df.count())
print("Raw output path:", raw_orders_path)

orders_df.write \
    .mode("overwrite") \
    .parquet(raw_orders_path)

print("Raw Layer write completed")

# ============================================================
# 10. Verify Raw Layer
# ============================================================

raw_orders_df = spark.read.parquet(raw_orders_path)

print("\nRaw Layer Verification")
print("Raw row count:", raw_orders_df.count())

print("\nRaw Schema:")
raw_orders_df.printSchema()

print("\nRaw Sample:")
raw_orders_df.show(5, truncate=False)


# ============================================================
# 11. Stop Spark
# ============================================================

spark.stop()

print("\nBatch ingestion job completed successfully")
spark.stop()

print("\nBatch ingestion job completed successfully")