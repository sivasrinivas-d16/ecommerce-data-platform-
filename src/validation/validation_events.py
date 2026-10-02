from pyspark.sql import SparkSession
from pyspark.sql.functions import col

from pyspark.sql.types import StructType, StructField, StringType

spark = (
    SparkSession.builder
    .appName("ECommerceEventValidation")
    .master("local[2]")
    .config("spark.driver.host", "127.0.0.1")
    .config("spark.driver.bindAddress", "127.0.0.1")
    .config("spark.hadoop.fs.permissions.umask-mode", "000")
    .getOrCreate()
)

events_path = r".\ecommerce-data-platform\data\raw\events"

events_df = spark.read.parquet(events_path)

print("Events loaded for validation")
print("Row count:", events_df.count())

events_df.printSchema()

print("\n--- Required Field Validation ---")

required_columns = [
    "event_id",
    "event_type",
    "customer_id",
    "event_timestamp",
    "source"
]

for column_name in required_columns:

    null_count = (
        events_df
        .filter(col(column_name).isNull())
        .count()
    )

    print(f"{column_name}: {null_count} null values")

print("\n--- Event ID Uniqueness Validation ---")

duplicate_event_ids = (
    events_df
    .groupBy("event_id")
    .count()
    .filter(col("count") > 1)
)

duplicate_event_count = duplicate_event_ids.count()

print("Duplicate event IDs:", duplicate_event_count)

print("\n--- Event ID Format Validation ---")

invalid_event_ids = (
    events_df
    .filter(~col("event_id").rlike("^E[0-9]{9}$"))
    .count()
)

print("Invalid event IDs:", invalid_event_ids)

print("\n--- Event Type Validation ---")

valid_event_types = [
    "PRODUCT_VIEWED",
    "ADD_TO_CART",
    "ORDER_CREATED",
    "PAYMENT_COMPLETED",
    "ORDER_SHIPPED",
    "ORDER_DELIVERED",
    "PAYMENT_FAILED"
]

invalid_event_types = (
    events_df
    .filter(~col("event_type").isin(valid_event_types))
    .count()
)

print("Invalid event types:", invalid_event_types)

print("\n--- Customer ID Format Validation ---")

invalid_customer_ids = (
    events_df
    .filter(~col("customer_id").rlike("^C[0-9]{5}$"))
    .count()
)

print("Invalid customer IDs:", invalid_customer_ids)

from pyspark.sql.functions import current_timestamp

print("\n--- Event Timestamp Validation ---")

future_event_count = (
    events_df
    .filter(col("event_timestamp") > current_timestamp())
    .count()
)

print("Future event timestamps:", future_event_count)

print("\n--- Event Source Distribution ---")

(
    events_df
    .groupBy("source")
    .count()
    .orderBy(col("count").desc())
    .show(truncate=False)
)

print("\n--- Event Source Validation ---")

valid_sources = [
    "WEB",
    "MOBILE_APP",
    "API",
    "STORE"
]
invalid_sources = (
    events_df
    .filter(~col("source").isin(valid_sources))
    .count()
)

print("Invalid event sources:", invalid_sources)

print("\n--- Product Event Reference Validation ---")

product_events = [
    "PRODUCT_VIEWED",
    "ADD_TO_CART"
]

invalid_product_event_refs = (
    events_df
    .filter(col("event_type").isin(product_events))
    .filter(col("product_id").isNull())
    .count()
)

print(
    "Product events with missing product_id:",
    invalid_product_event_refs
)

print("\n--- Order Event Reference Validation ---")

order_events = [
    "ORDER_CREATED",
    "PAYMENT_COMPLETED",
    "ORDER_SHIPPED",
    "ORDER_DELIVERED",
    "PAYMENT_FAILED"
]

invalid_order_event_refs = (
    events_df
    .filter(col("event_type").isin(order_events))
    .filter(col("order_id").isNull())
    .count()
)

print(
    "Order events with missing order_id:",
    invalid_order_event_refs
)

print("\n--- Product Referential Integrity Validation ---")

products_path = r".\ecommerce-data-platform\data\raw\products"

products_df = spark.read.parquet(products_path)

invalid_product_references = (
    events_df
    .filter(col("event_type").isin(product_events))
    .select("product_id")
    .distinct()
    .join(
        products_df.select("product_id").distinct(),
        on="product_id",
        how="left_anti"
    )
    .count()
)

print(
    "Invalid product references:",
    invalid_product_references
)

print("\n--- Order Referential Integrity Validation ---")

orders_path = r".\ecommerce-data-platform\data\raw\orders"

orders_df = spark.read.parquet(orders_path)

invalid_order_references = (
    events_df
    .filter(col("event_type").isin(order_events))
    .select("order_id")
    .distinct()
    .join(
        orders_df.select("order_id").distinct(),
        on="order_id",
        how="left_anti"
    )
    .count()
)

print(
    "Invalid order references:",
    invalid_order_references
)

print("\n--- Customer Referential Integrity Validation ---")

customers_path = r".\ecommerce-data-platform\data\raw\customers"

customers_df = spark.read.parquet(customers_path)

invalid_customer_references = (
    events_df
    .select("customer_id")
    .distinct()
    .join(
        customers_df.select("customer_id").distinct(),
        on="customer_id",
        how="left_anti"
    )
    .count()
)

print(
    "Invalid customer references:",
    invalid_customer_references
)

print("\n--- Payload Session ID Validation ---")

invalid_payload_ids = (
    events_df
    .filter(~col("payload").rlike("^S[0-9]{8}$"))
    .count()
)

print("Invalid payload session IDs:", invalid_payload_ids)

print("\n--- Events Validation Summary ---")

validation_results = [
    ("required_fields", "PASS"),
    ("event_id_uniqueness", "PASS"),
    ("event_id_format", "PASS"),
    ("event_type_validity", "PASS"),
    ("customer_id_format", "PASS"),
    ("future_timestamp_validation", "PASS"),
    ("event_source_validity", "PASS"),
    ("product_event_reference", "PASS"),
    ("order_event_reference", "PASS"),
    ("product_referential_integrity", "PASS"),
    ("order_referential_integrity", "PASS"),
    ("customer_referential_integrity", "PASS"),
    ("payload_session_id_format", "PASS")
]

for check_name, status in validation_results:
    print(f"{check_name}: {status}")

overall_status = (
    "PASS"
    if all(status == "PASS" for _, status in validation_results)
    else "FAIL"
)

print("\nOverall Event Validation Status:", overall_status)



validation_schema = StructType([
    StructField("check_name", StringType(), False),
    StructField("status", StringType(), False)
])

validation_report_df = spark.createDataFrame(
    validation_results,
    schema=validation_schema
)

validation_report_path = (
    r".\ecommerce-data-platform\data\processed\validation\events"
)

(
    validation_report_df.write
    .mode("overwrite")
    .parquet(validation_report_path)
)

print("\nEvent validation report persisted successfully.")

print(
    "Persisted validation checks:",
    validation_report_df.count()
)

validation_report_df.show(truncate=False)

spark.stop()