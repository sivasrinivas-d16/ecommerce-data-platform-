from pyspark.sql import SparkSession
from pyspark.sql.functions import trim, upper, col
from pyspark.sql.functions import to_date, year, month, dayofmonth, hour
from pyspark.sql.functions import when



spark = (
    SparkSession.builder
    .appName("ECommerceEventTransformation")
    .master("local[2]")
    .config("spark.driver.host", "127.0.0.1")
    .config("spark.driver.bindAddress", "127.0.0.1")
    .config("spark.hadoop.fs.permissions.umask-mode", "000")
    .getOrCreate()
)

raw_events_path = r".\ecommerce-data-platform\data\raw\events"

events_df = spark.read.parquet(raw_events_path)

print("\n--- Raw Events ---")
events_df.printSchema()

print(f"Raw event count: {events_df.count()}")

events_df.show(10, truncate=False)

events_df = (
    events_df
    .withColumn("event_type", upper(trim(col("event_type"))))
    .withColumn("source", upper(trim(col("source"))))
    .withColumn("payload", trim(col("payload")))
    .withColumn("customer_id", trim(col("customer_id")))
    .withColumn("order_id", trim(col("order_id")))
    .withColumn("product_id", trim(col("product_id")))
)

print("\n--- Standardized Events ---")

events_df.show(10, truncate=False)

events_df = (
    events_df
    .withColumn("event_date", to_date(col("event_timestamp")))
    .withColumn("event_year", year(col("event_timestamp")))
    .withColumn("event_month", month(col("event_timestamp")))
    .withColumn("event_day", dayofmonth(col("event_timestamp")))
    .withColumn("event_hour", hour(col("event_timestamp")))
)

print("\n--- Event Date/Time Transformations ---")

events_df.select(
    "event_id",
    "event_timestamp",
    "event_date",
    "event_year",
    "event_month",
    "event_day",
    "event_hour"
).show(10, truncate=False)

events_df = events_df.withColumn(
    "event_category",
    when(
        col("event_type").isin("PRODUCT_VIEWED", "ADD_TO_CART"),
        "BROWSING"
    )
    .when(
        col("event_type") == "ORDER_CREATED",
        "ORDER"
    )
    .when(
        col("event_type").isin("PAYMENT_COMPLETED", "PAYMENT_FAILED"),
        "PAYMENT"
    )
    .when(
        col("event_type").isin("ORDER_SHIPPED", "ORDER_DELIVERED"),
        "FULFILLMENT"
    )
    .otherwise("OTHER")
)

print("\n--- Event Category ---")

events_df.select(
    "event_id",
    "event_type",
    "event_category"
).show(10, truncate=False)

print("\n--- Event Transformation Verification ---")

print(f"Transformed event count: {events_df.count()}")

print("\nEvent Category Distribution:")
events_df.groupBy("event_category").count().show()

print("\nEvent Type Distribution:")
events_df.groupBy("event_type").count().show()

print("\nFinal Event Schema:")
events_df.printSchema()

curated_events_path = r".\ecommerce-data-platform\data\curated\events"

(
    events_df
    .write
    .mode("overwrite")
    .parquet(curated_events_path)
)

print("Curated events written successfully.")

curated_events_df = spark.read.parquet(curated_events_path)

print("\n--- Curated Events Verification ---")
print(f"Curated event count: {curated_events_df.count()}")

curated_events_df.printSchema()

curated_events_df.show(5, truncate=False)


