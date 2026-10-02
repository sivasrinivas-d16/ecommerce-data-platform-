from pyspark.sql import SparkSession
from pyspark.sql.functions import col


# =========================================================
# Spark Session
# =========================================================

spark = (
    SparkSession.builder
    .appName("QualityIncrementalOrders")
    .master("local[*]")
    .config("spark.driver.host", "127.0.0.1")
    .config("spark.driver.bindAddress", "127.0.0.1")
    .getOrCreate()
)

spark.sparkContext.setLogLevel("WARN")


# =========================================================
# Path
# =========================================================

incremental_path = (
    r".\ecommerce-data-platform\data\raw\incremental\orders_batch_002"
)


# =========================================================
# Read Incremental Batch
# =========================================================

orders_df = (
    spark.read
    .parquet(incremental_path)
)

total_records = orders_df.count()

print(f"Incremental records: {total_records}")


# =========================================================
# Quality Results
# =========================================================

quality_results = []


# =========================================================
# 1. Completeness
# =========================================================

required_columns = [
    "order_id",
    "customer_id",
    "product_id",
    "quantity",
    "unit_price",
    "order_amount",
    "order_status",
    "payment_status",
    "order_timestamp"
]

total_required_values = (
    total_records * len(required_columns)
)

missing_required_values = 0

for column in required_columns:

    missing_count = (
        orders_df
        .filter(
            col(column).isNull()
        )
        .count()
    )

    missing_required_values += missing_count


completeness_score = (
    (
        total_required_values
        - missing_required_values
    )
    / total_required_values
) * 100


quality_results.append(
    (
        "Completeness",
        completeness_score
    )
)


# =========================================================
# 2. Order ID Uniqueness
# =========================================================

distinct_order_ids = (
    orders_df
    .select("order_id")
    .distinct()
    .count()
)

order_id_uniqueness = (
    distinct_order_ids
    / total_records
) * 100


quality_results.append(
    (
        "Order ID uniqueness",
        order_id_uniqueness
    )
)


# =========================================================
# 3. Order ID Validity
# =========================================================

valid_order_ids = (
    orders_df
    .filter(
        col("order_id").rlike(
            r"^O[0-9]{8}$"
        )
    )
    .count()
)

order_id_validity = (
    valid_order_ids
    / total_records
) * 100


quality_results.append(
    (
        "Order ID validity",
        order_id_validity
    )
)


# =========================================================
# 4. Quantity Validity
# =========================================================

valid_quantity = (
    orders_df
    .filter(
        col("quantity") > 0
    )
    .count()
)

quantity_validity = (
    valid_quantity
    / total_records
) * 100


quality_results.append(
    (
        "Quantity validity",
        quantity_validity
    )
)


# =========================================================
# 5. Unit Price Validity
# =========================================================

valid_unit_price = (
    orders_df
    .filter(
        col("unit_price") >= 0
    )
    .count()
)

unit_price_validity = (
    valid_unit_price
    / total_records
) * 100


quality_results.append(
    (
        "Unit price validity",
        unit_price_validity
    )
)


# =========================================================
# 6. Order Amount Validity
# =========================================================

valid_order_amount = (
    orders_df
    .filter(
        col("order_amount") >= 0
    )
    .count()
)

order_amount_validity = (
    valid_order_amount
    / total_records
) * 100


quality_results.append(
    (
        "Order amount validity",
        order_amount_validity
    )
)


# =========================================================
# 7. Order Status Validity
# =========================================================

valid_order_statuses = [
    "COMPLETED",
    "CANCELLED",
    "PENDING",
    "SHIPPED",
    "PROCESSING",
    "RETURNED"
]

valid_status_count = (
    orders_df
    .filter(
        col("order_status").isin(
            valid_order_statuses
        )
    )
    .count()
)

order_status_validity = (
    valid_status_count
    / total_records
) * 100


quality_results.append(
    (
        "Order status validity",
        order_status_validity
    )
)


# =========================================================
# 8. Payment Status Validity
# =========================================================

valid_payment_statuses = [
    "PAID",
    "PENDING",
    "FAILED",
    "REFUNDED"
]

valid_payment_status_count = (
    orders_df
    .filter(
        col("payment_status").isin(
            valid_payment_statuses
        )
    )
    .count()
)

payment_status_validity = (
    valid_payment_status_count
    / total_records
) * 100


quality_results.append(
    (
        "Payment status validity",
        payment_status_validity
    )
)


# =========================================================
# 9. Timestamp Validity
# =========================================================

valid_timestamp_count = (
    orders_df
    .filter(
        col("order_timestamp").isNotNull()
    )
    .count()
)

timestamp_validity = (
    valid_timestamp_count
    / total_records
) * 100


quality_results.append(
    (
        "Timestamp validity",
        timestamp_validity
    )
)


# =========================================================
# Display Quality Results
# =========================================================

print("\n--- Incremental Order Quality ---")

for quality_name, score in quality_results:

    print(
        f"{quality_name:<25} : "
        f"{score:.2f}%"
    )


# =========================================================
# Overall Quality Score
# =========================================================

overall_quality_score = (
    sum(
        score
        for _, score in quality_results
    )
    / len(quality_results)
)


print("\n----------------------------------------")

print(
    f"Overall Quality Score: "
    f"{overall_quality_score:.2f}%"
)


# =========================================================
# Quality Status
# =========================================================

if overall_quality_score >= 95:

    print("Quality Status: PASS")

elif overall_quality_score >= 80:

    print("Quality Status: WARNING")

else:

    print("Quality Status: FAIL")


# =========================================================
# Stop Spark
# =========================================================

spark.stop()