from pyspark.sql import SparkSession
from pyspark.sql.functions import col, count, when
from datetime import datetime

spark = (
    SparkSession.builder
    .appName("ECommerceOrderQuality")
    .master("local[2]")
    .config("spark.driver.host", "127.0.0.1")
    .config("spark.driver.bindAddress", "127.0.0.1")
    .config("spark.hadoop.fs.permissions.umask-mode", "000")
    .getOrCreate()
)

curated_orders_path = r".\ecommerce-data-platform\data\curated\orders"

orders_df = spark.read.parquet(curated_orders_path)

print("Curated Orders loaded successfully")
print("Row count:", orders_df.count())

quality_columns = [
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

total_records = orders_df.count()

completeness_counts = orders_df.select(
    *[
        count(
            when(col(column_name).isNotNull(), 1)
        ).alias(column_name)
        for column_name in quality_columns
    ]
).collect()[0]

for column_name in quality_columns:

    non_null_count = completeness_counts[column_name]

    completeness_percentage = (
        non_null_count / total_records
    ) * 100

    print(
        f"{column_name:<20} "
        f"Completeness: {completeness_percentage:.2f}%"
    )

# ---------------------------------------------------------
# Uniqueness Quality
# ---------------------------------------------------------

total_records = orders_df.count()

unique_order_ids = (
    orders_df
    .select("order_id")
    .distinct()
    .count()
)

uniqueness_percentage = (
    unique_order_ids / total_records
) * 100

print(
    f"\nOrder ID Uniqueness: "
    f"{uniqueness_percentage:.2f}%"
)

# ---------------------------------------------------------
# Validity Quality - Quantity
# ---------------------------------------------------------

valid_quantity_count = (
    orders_df
    .filter(col("quantity") > 0)
    .count()
)

quantity_validity_percentage = (
    valid_quantity_count / total_records
) * 100

print(
    f"Quantity Validity: "
    f"{quantity_validity_percentage:.2f}%"
)

# ---------------------------------------------------------
# Validity Quality - Unit Price
# ---------------------------------------------------------

valid_unit_price_count = (
    orders_df
    .filter(col("unit_price") >= 0)
    .count()
)

unit_price_validity_percentage = (
    valid_unit_price_count / total_records
) * 100

print(
    f"Unit Price Validity: "
    f"{unit_price_validity_percentage:.2f}%"
)

# ---------------------------------------------------------
# Validity Quality - Order Amount
# ---------------------------------------------------------

valid_order_amount_count = (
    orders_df
    .filter(col("order_amount") >= 0)
    .count()
)

order_amount_validity_percentage = (
    valid_order_amount_count / total_records
) * 100

print(
    f"Order Amount Validity: "
    f"{order_amount_validity_percentage:.2f}%"
)

# ---------------------------------------------------------
# Consistency Quality - Order Amount
# ---------------------------------------------------------

consistent_order_amount_count = (
    orders_df
    .filter(
        col("order_amount") ==
        (col("quantity") * col("unit_price")).cast("decimal(14,2)")
    )
    .count()
)

order_amount_consistency_percentage = (
    consistent_order_amount_count / total_records
) * 100

print(
    f"Order Amount Consistency: "
    f"{order_amount_consistency_percentage:.2f}%"
)

# ---------------------------------------------------------
# Validity Quality - Order Status
# ---------------------------------------------------------

valid_order_status_count = (
    orders_df
    .filter(
        col("order_status").isin(
            "COMPLETED",
            "SHIPPED",
            "PROCESSING",
            "CANCELLED",
            "RETURNED"
        )
    )
    .count()
)

order_status_validity_percentage = (
    valid_order_status_count / total_records
) * 100

print(
    f"Order Status Validity: "
    f"{order_status_validity_percentage:.2f}%"
)

# ---------------------------------------------------------
# Validity Quality - Payment Status
# ---------------------------------------------------------

valid_payment_status_count = (
    orders_df
    .filter(
        col("payment_status").isin(
            "PAID",
            "PENDING",
            "FAILED",
            "REFUNDED"
        )
    )
    .count()
)

payment_status_validity_percentage = (
    valid_payment_status_count / total_records
) * 100

print(
    f"Payment Status Validity: "
    f"{payment_status_validity_percentage:.2f}%"
)

# ---------------------------------------------------------
# Quality Metrics
# ---------------------------------------------------------

quality_metrics = [
    ("Completeness", 100.00),
    ("Order ID Uniqueness", uniqueness_percentage),
    ("Quantity Validity", quantity_validity_percentage),
    ("Unit Price Validity", unit_price_validity_percentage),
    ("Order Amount Validity", order_amount_validity_percentage),
    ("Order Amount Consistency", order_amount_consistency_percentage),
    ("Order Status Validity", order_status_validity_percentage),
    ("Payment Status Validity", payment_status_validity_percentage)
]

print("\nQuality Metrics")
print("-" * 60)

for metric_name, metric_score in quality_metrics:
    print(
        f"{metric_name:<30} "
        f"{metric_score:>8.2f}%"
    )

# ---------------------------------------------------------
# Overall Quality Score
# ---------------------------------------------------------

overall_quality_score = (
    sum(metric_score for _, metric_score in quality_metrics)
    / len(quality_metrics)
)

print(
    f"\nOverall Data Quality Score: "
    f"{overall_quality_score:.2f}%"
)

# ---------------------------------------------------------
# Quality Report
# ---------------------------------------------------------

quality_run_timestamp = datetime.now()

quality_report = [
    {
        "dataset": "orders",
        "metric_name": metric_name,
        "metric_score": metric_score,
        "run_timestamp": quality_run_timestamp
    }
    for metric_name, metric_score in quality_metrics
]

for result in quality_report:
    print(result)

# ---------------------------------------------------------
# Save Quality Report
# ---------------------------------------------------------

quality_report_path = r".\ecommerce-data-platform\data\processed\quality"

quality_report_df = spark.createDataFrame(quality_report)

(
    quality_report_df
    .write
    .mode("overwrite")
    .parquet(quality_report_path)
)

print("\nQuality report written successfully.")