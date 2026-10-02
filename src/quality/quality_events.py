from pyspark.sql import SparkSession
from pyspark.sql.functions import col

from pyspark.sql.functions import current_timestamp
from sklearn.metrics import completeness_score


spark = (
    SparkSession.builder
    .appName("ECommerceEventQuality")
    .master("local[2]")
    .config("spark.driver.host", "127.0.0.1")
    .config("spark.driver.bindAddress", "127.0.0.1")
    .config("spark.hadoop.fs.permissions.umask-mode", "000")
    .getOrCreate()
)

events_path = r".\ecommerce-data-platform\data\raw\events"

events_df = spark.read.parquet(events_path)

print("Events loaded for quality assessment")
print("Row count:", events_df.count())

events_df.printSchema()

quality_columns = [
    "event_id",
    "event_type",
    "customer_id",
    "event_timestamp",
    "source",
    "payload"
]

print("\n--- Completeness Quality ---")

quality_columns = [
    "event_id",
    "event_type",
    "customer_id",
    "event_timestamp",
    "source",
    "payload"
]

total_records = events_df.count()

completeness_scores = []

for column_name in quality_columns:

    non_null_count = (
        events_df
        .filter(col(column_name).isNotNull())
        .count()
    )

    completeness = (
        non_null_count / total_records
    ) * 100

    completeness_scores.append(completeness)

    print(
        f"{column_name}: "
        f"{completeness:.2f}% completeness"
    )

print("\n--- Event ID Uniqueness Quality ---")

distinct_event_ids = (
    events_df
    .select("event_id")
    .distinct()
    .count()
)

event_id_uniqueness = (
    distinct_event_ids / total_records
) * 100

print(
    f"event_id uniqueness: "
    f"{event_id_uniqueness:.2f}%"
)

print("\n--- Event Type Validity Quality ---")

valid_event_types = [
    "PRODUCT_VIEWED",
    "ADD_TO_CART",
    "ORDER_CREATED",
    "PAYMENT_COMPLETED",
    "ORDER_SHIPPED",
    "ORDER_DELIVERED",
    "PAYMENT_FAILED"
]

valid_event_type_count = (
    events_df
    .filter(col("event_type").isin(valid_event_types))
    .count()
)

event_type_validity = (
    valid_event_type_count / total_records
) * 100

print(
    f"event_type validity: "
    f"{event_type_validity:.2f}%"
)

print("\n--- Customer ID Validity Quality ---")

valid_customer_id_count = (
    events_df
    .filter(col("customer_id").rlike("^C[0-9]{5}$"))
    .count()
)

customer_id_validity = (
    valid_customer_id_count / total_records
) * 100

print(
    f"customer_id validity: "
    f"{customer_id_validity:.2f}%"
)

print("\n--- Event Timestamp Validity Quality ---")

valid_timestamp_count = (
    events_df
    .filter(col("event_timestamp") <= current_timestamp())
    .count()
)

event_timestamp_validity = (
    valid_timestamp_count / total_records
) * 100

print(
    f"event_timestamp validity: "
    f"{event_timestamp_validity:.2f}%"
)

print("\n--- Event Source Validity Quality ---")

valid_sources = [
    "WEB",
    "MOBILE_APP",
    "API",
    "STORE"
]

valid_source_count = (
    events_df
    .filter(col("source").isin(valid_sources))
    .count()
)

event_source_validity = (
    valid_source_count / total_records
) * 100

print(
    f"event_source validity: "
    f"{event_source_validity:.2f}%"
)

print("\n--- Product Event Reference Quality ---")

product_events = [
    "PRODUCT_VIEWED",
    "ADD_TO_CART"
]

product_event_count = (
    events_df
    .filter(col("event_type").isin(product_events))
    .count()
)

valid_product_event_refs = (
    events_df
    .filter(col("event_type").isin(product_events))
    .filter(col("product_id").isNotNull())
    .count()
)

product_event_reference_validity = (
    valid_product_event_refs / product_event_count
) * 100

print(
    f"product_event reference validity: "
    f"{product_event_reference_validity:.2f}%"
)

print("\n--- Order Event Reference Quality ---")

order_events = [
    "ORDER_CREATED",
    "PAYMENT_COMPLETED",
    "ORDER_SHIPPED",
    "ORDER_DELIVERED",
    "PAYMENT_FAILED"
]

order_event_count = (
    events_df
    .filter(col("event_type").isin(order_events))
    .count()
)

valid_order_event_refs = (
    events_df
    .filter(col("event_type").isin(order_events))
    .filter(col("order_id").isNotNull())
    .count()
)

order_event_reference_validity = (
    valid_order_event_refs / order_event_count
) * 100

print(
    f"order_event reference validity: "
    f"{order_event_reference_validity:.2f}%"
)

print("\n--- Product Referential Integrity Quality ---")

products_path = r".\ecommerce-data-platform\data\raw\products"

products_df = spark.read.parquet(products_path)

product_event_ids = (
    events_df
    .filter(col("event_type").isin(product_events))
    .select("product_id")
    .distinct()
)

valid_product_ids = (
    product_event_ids
    .join(
        products_df.select("product_id").distinct(),
        on="product_id",
        how="inner"
    )
    .count()
)

total_product_ids = product_event_ids.count()

product_referential_integrity = (
    valid_product_ids / total_product_ids
) * 100

print(
    f"product referential integrity: "
    f"{product_referential_integrity:.2f}%"
)

print("\n--- Order Referential Integrity Quality ---")

orders_path = r".\ecommerce-data-platform\data\raw\orders"

orders_df = spark.read.parquet(orders_path)

order_event_ids = (
    events_df
    .filter(col("event_type").isin(order_events))
    .select("order_id")
    .distinct()
)

valid_order_ids = (
    order_event_ids
    .join(
        orders_df.select("order_id").distinct(),
        on="order_id",
        how="inner"
    )
    .count()
)

total_order_ids = order_event_ids.count()

order_referential_integrity = (
    valid_order_ids / total_order_ids
) * 100

print(
    f"order referential integrity: "
    f"{order_referential_integrity:.2f}%"
)

print("\n--- Customer Referential Integrity Quality ---")

customers_path = r".\ecommerce-data-platform\data\raw\customers"

customers_df = spark.read.parquet(customers_path)

event_customer_ids = (
    events_df
    .select("customer_id")
    .distinct()
)

valid_customer_ids = (
    event_customer_ids
    .join(
        customers_df.select("customer_id").distinct(),
        on="customer_id",
        how="inner"
    )
    .count()
)

total_customer_ids = event_customer_ids.count()

customer_referential_integrity = (
    valid_customer_ids / total_customer_ids
) * 100

print(
    f"customer referential integrity: "
    f"{customer_referential_integrity:.2f}%"
)

print("\n--- Payload Session ID Validity Quality ---")

valid_payload_count = (
    events_df
    .filter(col("payload").rlike("^S[0-9]{8}$"))
    .count()
)

payload_session_validity = (
    valid_payload_count / total_records
) * 100

print(
    f"payload session ID validity: "
    f"{payload_session_validity:.2f}%"
)


print("\n--- Current Quality Metric Variables ---")

print("completeness_score:", completeness_score)
print("event_id_uniqueness:", event_id_uniqueness)
print("event_type_validity:", event_type_validity)
print("customer_id_validity:", customer_id_validity)
print("event_timestamp_validity:", event_timestamp_validity)
print("event_source_validity:", event_source_validity)
print("product_event_reference_validity:", product_event_reference_validity)
print("order_event_reference_validity:", order_event_reference_validity)
print("product_referential_integrity:", product_referential_integrity)
print("order_referential_integrity:", order_referential_integrity)
print("customer_referential_integrity:", customer_referential_integrity)
print("payload_session_validity:", payload_session_validity)

print("\n--- Overall Events Quality Score ---")

completeness_quality = 100.0

overall_quality_score = (
    completeness_quality
    + event_id_uniqueness
    + event_type_validity
    + customer_id_validity
    + event_timestamp_validity
    + event_source_validity
    + product_event_reference_validity
    + order_event_reference_validity
    + product_referential_integrity
    + order_referential_integrity
    + customer_referential_integrity
    + payload_session_validity
) / 12

print(
    f"Overall Events Quality Score: "
    f"{overall_quality_score:.2f}%"
)

quality_report = [
    ("completeness", completeness_quality),
    ("event_id_uniqueness", event_id_uniqueness),
    ("event_type_validity", event_type_validity),
    ("customer_id_validity", customer_id_validity),
    ("event_timestamp_validity", event_timestamp_validity),
    ("event_source_validity", event_source_validity),
    ("product_event_reference_validity", product_event_reference_validity),
    ("order_event_reference_validity", order_event_reference_validity),
    ("product_referential_integrity", product_referential_integrity),
    ("order_referential_integrity", order_referential_integrity),
    ("customer_referential_integrity", customer_referential_integrity),
    ("payload_session_validity", payload_session_validity),
    ("overall_quality_score", overall_quality_score)
]

print("\n--- Events Quality Report ---")

for metric, score in quality_report:
    print(f"{metric}: {score:.2f}%")

quality_report_df = spark.createDataFrame(
    quality_report,
    ["metric", "score"]
)

print("\n--- Quality Report DataFrame ---")
quality_report_df.show(truncate=False)

quality_output_path = r".\ecommerce-data-platform\data\processed\quality\events"

(
    quality_report_df
    .write
    .mode("overwrite")
    .parquet(quality_output_path)
)

print("Events quality report persisted successfully.")

saved_quality_df = spark.read.parquet(quality_output_path)

print("\n--- Persisted Events Quality Report ---")
saved_quality_df.show(truncate=False)