from pyspark.sql import SparkSession
from pyspark.sql.functions import col


# =========================================================
# Spark Session
# =========================================================

spark = (
    SparkSession.builder
    .appName("ValidationIncrementalOrders")
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
# Validation Results
# =========================================================

validation_results = []


# =========================================================
# 1. Required Fields
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

required_nulls = 0

for column in required_columns:

    null_count = (
        orders_df
        .filter(
            col(column).isNull()
        )
        .count()
    )

    required_nulls += null_count


validation_results.append(
    (
        "Required fields",
        required_nulls == 0,
        required_nulls
    )
)


# =========================================================
# 2. Duplicate Order IDs
# =========================================================

duplicate_order_ids = (
    orders_df
    .groupBy("order_id")
    .count()
    .filter(
        col("count") > 1
    )
    .count()
)

validation_results.append(
    (
        "Duplicate order IDs",
        duplicate_order_ids == 0,
        duplicate_order_ids
    )
)


# =========================================================
# 3. Order ID Format
# =========================================================

invalid_order_id_format = (
    orders_df
    .filter(
        ~col("order_id").rlike(
            r"^O[0-9]{8}$"
        )
    )
    .count()
)

validation_results.append(
    (
        "Order ID format",
        invalid_order_id_format == 0,
        invalid_order_id_format
    )
)


# =========================================================
# 4. Quantity > 0
# =========================================================

invalid_quantity = (
    orders_df
    .filter(
        col("quantity") <= 0
    )
    .count()
)

validation_results.append(
    (
        "Quantity > 0",
        invalid_quantity == 0,
        invalid_quantity
    )
)


# =========================================================
# 5. Unit Price >= 0
# =========================================================

invalid_unit_price = (
    orders_df
    .filter(
        col("unit_price") < 0
    )
    .count()
)

validation_results.append(
    (
        "Unit price >= 0",
        invalid_unit_price == 0,
        invalid_unit_price
    )
)


# =========================================================
# 6. Order Amount >= 0
# =========================================================

invalid_order_amount = (
    orders_df
    .filter(
        col("order_amount") < 0
    )
    .count()
)

validation_results.append(
    (
        "Order amount >= 0",
        invalid_order_amount == 0,
        invalid_order_amount
    )
)


# =========================================================
# 7. Valid Order Status
# =========================================================

valid_order_statuses = [
    "COMPLETED",
    "CANCELLED",
    "PENDING",
    "SHIPPED",
    "PROCESSING",
    "RETURNED"
]

invalid_order_status = (
    orders_df
    .filter(
        ~col("order_status").isin(
            valid_order_statuses
        )
    )
    .count()
)

validation_results.append(
    (
        "Valid order status",
        invalid_order_status == 0,
        invalid_order_status
    )
)


# =========================================================
# 8. Valid Payment Status
# =========================================================

valid_payment_statuses = [
    "PAID",
    "PENDING",
    "FAILED",
    "REFUNDED"
]

invalid_payment_status = (
    orders_df
    .filter(
        ~col("payment_status").isin(
            valid_payment_statuses
        )
    )
    .count()
)

validation_results.append(
    (
        "Valid payment status",
        invalid_payment_status == 0,
        invalid_payment_status
    )
)


# =========================================================
# 9. Order Timestamp
# =========================================================

invalid_timestamp = (
    orders_df
    .filter(
        col("order_timestamp").isNull()
    )
    .count()
)

validation_results.append(
    (
        "Order timestamp",
        invalid_timestamp == 0,
        invalid_timestamp
    )
)


# =========================================================
# Display Validation Results
# =========================================================

print("\n--- Incremental Order Validation ---")

overall_pass = True

for check_name, passed, invalid_count in validation_results:

    status = "PASS" if passed else "FAIL"

    print(
        f"{check_name:<25} : "
        f"{status:<5} "
        f"(invalid = {invalid_count})"
    )

    if not passed:
        overall_pass = False


# =========================================================
# Overall Result
# =========================================================

print("\n----------------------------------------")

if overall_pass:

    print("Overall Validation: PASS")

else:

    print("Overall Validation: FAIL")


# =========================================================
# Stop Spark
# =========================================================

spark.stop()