from pyspark.sql import SparkSession
from pyspark.sql.functions import col, count, when

from datetime import datetime

# ---------------------------------------------------------
# Spark Session
# ---------------------------------------------------------

spark = (
    SparkSession.builder
    .appName("ECommerceProductQuality")
    .master("local[2]")
    .config("spark.driver.host", "127.0.0.1")
    .config("spark.driver.bindAddress", "127.0.0.1")
    .config("spark.hadoop.fs.permissions.umask-mode", "000")
    .getOrCreate()
)


# ---------------------------------------------------------
# Load Products from Raw Layer
# ---------------------------------------------------------

products_path = r".\ecommerce-data-platform\data\raw\products"

products_df = spark.read.parquet(products_path)

total_records = products_df.count()

print("Products loaded from Raw layer")
print("Total product records:", total_records)

print("\nProduct Schema:")
products_df.printSchema()


# ---------------------------------------------------------
# Completeness Measurement
# ---------------------------------------------------------

quality_columns = [
    "product_id",
    "product_name",
    "category",
    "subcategory",
    "brand",
    "price",
    "stock_quantity",
    "product_status",
    "created_date"
]

completeness_counts = products_df.select(
    *[
        count(
            when(col(column_name).isNotNull(), 1)
        ).alias(column_name)
        for column_name in quality_columns
    ]
).collect()[0]


print("\nProduct Completeness:")

for column_name in quality_columns:

    non_null_count = completeness_counts[column_name]

    completeness_percentage = (
        non_null_count / total_records
    ) * 100

    print(
        f"{column_name:<18} "
        f"Completeness: {completeness_percentage:.2f}%"
    )

# ---------------------------------------------------------
# Product ID Uniqueness
# ---------------------------------------------------------

unique_product_ids = products_df.select(
    "product_id"
).distinct().count()

uniqueness_percentage = (
    unique_product_ids / total_records
) * 100

print("\nProduct ID Uniqueness:")
print("Unique product IDs:", unique_product_ids)
print(f"Uniqueness: {uniqueness_percentage:.2f}%")

# ---------------------------------------------------------
# Product ID Format Quality
# ---------------------------------------------------------

valid_product_id_count = products_df.filter(
    col("product_id").rlike(r"^P[0-9]{6}$")
).count()

product_id_validity_percentage = (
    valid_product_id_count / total_records
) * 100

print("\nProduct ID Format Quality:")
print("Valid product ID records:", valid_product_id_count)
print(
    f"Product ID Validity: "
    f"{product_id_validity_percentage:.2f}%"
)

# ---------------------------------------------------------
# Price Validity
# ---------------------------------------------------------

valid_price_count = products_df.filter(
    col("price") >= 0
).count()

price_validity_percentage = (
    valid_price_count / total_records
) * 100

print("\nPrice Validity:")
print("Valid price records:", valid_price_count)
print(
    f"Price Validity: "
    f"{price_validity_percentage:.2f}%"
)

# ---------------------------------------------------------
# Stock Quantity Validity
# ---------------------------------------------------------

valid_stock_quantity_count = products_df.filter(
    col("stock_quantity") >= 0
).count()

stock_quantity_validity_percentage = (
    valid_stock_quantity_count / total_records
) * 100

print("\nStock Quantity Validity:")
print(
    "Valid stock quantity records:",
    valid_stock_quantity_count
)

print(
    f"Stock Quantity Validity: "
    f"{stock_quantity_validity_percentage:.2f}%"
)

# ---------------------------------------------------------
# Product Status Validity
# ---------------------------------------------------------

valid_product_statuses = [
    "ACTIVE",
    "INACTIVE",
    "DISCONTINUED"
]

valid_product_status_count = products_df.filter(
    col("product_status").isin(valid_product_statuses)
).count()

product_status_validity_percentage = (
    valid_product_status_count / total_records
) * 100

print("\nProduct Status Validity:")
print(
    "Valid product status records:",
    valid_product_status_count
)

print(
    f"Product Status Validity: "
    f"{product_status_validity_percentage:.2f}%"
)

# ---------------------------------------------------------
# Created Date Validity
# ---------------------------------------------------------

from pyspark.sql.functions import current_date

valid_created_date_count = products_df.filter(
    col("created_date").isNotNull()
    & (col("created_date") <= current_date())
).count()

created_date_validity_percentage = (
    valid_created_date_count / total_records
) * 100

print("\nCreated Date Validity:")
print(
    "Valid created date records:",
    valid_created_date_count
)

print(
    f"Created Date Validity: "
    f"{created_date_validity_percentage:.2f}%"
)

# ---------------------------------------------------------
# Overall Product Quality Score
# ---------------------------------------------------------

quality_metrics = [
    ("Completeness", 100.00),
    ("Product ID Uniqueness", uniqueness_percentage),
    ("Product ID Validity", product_id_validity_percentage),
    ("Price Validity", price_validity_percentage),
    ("Stock Quantity Validity", stock_quantity_validity_percentage),
    ("Product Status Validity", product_status_validity_percentage),
    ("Created Date Validity", created_date_validity_percentage)
]

overall_quality_score = (
    sum(metric_score for _, metric_score in quality_metrics)
    / len(quality_metrics)
)

print("\nProduct Quality Metrics:")

for metric_name, metric_score in quality_metrics:
    print(
        f"{metric_name:<30} "
        f"Score: {metric_score:.2f}%"
    )

print(
    f"\nOverall Product Quality Score: "
    f"{overall_quality_score:.2f}%"
)

# ---------------------------------------------------------
# Quality Report
# ---------------------------------------------------------

quality_run_timestamp = datetime.now()

quality_report = [
    {
        "dataset": "products",
        "metric_name": metric_name,
        "metric_score": metric_score,
        "run_timestamp": quality_run_timestamp
    }
    for metric_name, metric_score in quality_metrics
]

print("\nQuality Report:")

for result in quality_report:
    print(result)

# ---------------------------------------------------------
# Persist Quality Report
# ---------------------------------------------------------

quality_report_path = (
    r".\ecommerce-data-platform\data\processed\quality\products"
)

quality_report_df = spark.createDataFrame(quality_report)

quality_report_df.write \
    .mode("overwrite") \
    .parquet(quality_report_path)

print("\nProduct quality report written successfully.")

# ---------------------------------------------------------
# Verify Persisted Quality Report
# ---------------------------------------------------------

saved_quality_report_df = spark.read.parquet(
    quality_report_path
)

print("\nPersisted Product Quality Report:")
saved_quality_report_df.show(
    truncate=False
)

print(
    "Persisted quality report count:",
    saved_quality_report_df.count()
)

# ---------------------------------------------------------
# Stop Spark
# ---------------------------------------------------------

spark.stop()

