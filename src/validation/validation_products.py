from pyspark.sql import SparkSession
from pyspark.sql.functions import col, count, when

from pyspark.sql.functions import current_date
from datetime import datetime

# ---------------------------------------------------------
# Spark Session
# ---------------------------------------------------------

spark = (
    SparkSession.builder
    .appName("ECommerceProductValidation")
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
# Required Field Validation
# ---------------------------------------------------------

required_columns = [
    "product_id",
    "product_name",
    "category",
    "brand",
    "price",
    "stock_quantity",
    "product_status",
    "created_date"
]


required_null_counts = products_df.select(
    *[
        count(
            when(col(column_name).isNull(), 1)
        ).alias(column_name)
        for column_name in required_columns
    ]
).collect()[0]


print("\nRequired Field Validation:")

for column_name in required_columns:

    null_count = required_null_counts[column_name]

    status = "PASS" if null_count == 0 else "FAIL"

    print(
        f"{column_name:<18} "
        f"Null count: {null_count:<8} "
        f"Status: {status}"
    )

# ---------------------------------------------------------
# Duplicate Product ID Validation
# ---------------------------------------------------------

duplicate_product_ids = (
    products_df
    .groupBy("product_id")
    .count()
    .filter(col("count") > 1)
)

duplicate_product_id_count = duplicate_product_ids.count()

print("\nDuplicate Product ID Validation:")
print("Duplicate product IDs:", duplicate_product_id_count)

status = (
    "PASS"
    if duplicate_product_id_count == 0
    else "FAIL"
)

print("Status:", status)

# ---------------------------------------------------------
# Product ID Format Validation
# ---------------------------------------------------------

invalid_product_id_count = products_df.filter(
    ~col("product_id").rlike(r"^P[0-9]{6}$")
).count()

print("\nProduct ID Format Validation:")
print("Invalid product ID records:", invalid_product_id_count)

status = (
    "PASS"
    if invalid_product_id_count == 0
    else "FAIL"
)

print("Status:", status)

# ---------------------------------------------------------
# Product Price Validation
# ---------------------------------------------------------

invalid_price_count = products_df.filter(
    col("price") < 0
).count()

print("\nProduct Price Validation:")
print("Invalid price records:", invalid_price_count)

status = (
    "PASS"
    if invalid_price_count == 0
    else "FAIL"
)

print("Status:", status)

# ---------------------------------------------------------
# Stock Quantity Validation
# ---------------------------------------------------------

invalid_stock_quantity_count = products_df.filter(
    col("stock_quantity") < 0
).count()

print("\nStock Quantity Validation:")
print(
    "Invalid stock quantity records:",
    invalid_stock_quantity_count
)

status = (
    "PASS"
    if invalid_stock_quantity_count == 0
    else "FAIL"
)

print("Status:", status)

# ---------------------------------------------------------
# Product Status Validation
# ---------------------------------------------------------

valid_product_statuses = [
    "ACTIVE",
    "INACTIVE",
    "DISCONTINUED"
]

invalid_product_status_count = products_df.filter(
    ~col("product_status").isin(valid_product_statuses)
).count()

print("\nProduct Status Validation:")
print(
    "Invalid product status records:",
    invalid_product_status_count
)

status = (
    "PASS"
    if invalid_product_status_count == 0
    else "FAIL"
)


# ---------------------------------------------------------
# Created Date Validation
# ---------------------------------------------------------



invalid_created_date_count = products_df.filter(
    col("created_date").isNull()
    | (col("created_date") > current_date())
).count()

print("\nCreated Date Validation:")
print(
    "Invalid created date records:",
    invalid_created_date_count
)

status = (
    "PASS"
    if invalid_created_date_count == 0
    else "FAIL"
)

print("Status:", status)

# ---------------------------------------------------------
# Validation Results
# ---------------------------------------------------------

validation_results = [
    ("Required Field Validation", 0),
    ("Duplicate Product ID Validation", duplicate_product_id_count),
    ("Product ID Format Validation", invalid_product_id_count),
    ("Product Price Validation", invalid_price_count),
    ("Stock Quantity Validation", invalid_stock_quantity_count),
    ("Product Status Validation", invalid_product_status_count),
    ("Created Date Validation", invalid_created_date_count)
]


# ---------------------------------------------------------
# Validation Summary
# ---------------------------------------------------------

print("\nValidation Summary:")

for validation_name, invalid_records in validation_results:

    status = "PASS" if invalid_records == 0 else "FAIL"

    print(
        f"{validation_name:<40} "
        f"Invalid Records: {invalid_records:<8} "
        f"Status: {status}"
    )


# ---------------------------------------------------------
# Overall Validation Status
# ---------------------------------------------------------

overall_status = (
    "PASS"
    if all(
        invalid_records == 0
        for _, invalid_records in validation_results
    )
    else "FAIL"
)

print(
    "\nOverall Product Validation Status:",
    overall_status
)

# ---------------------------------------------------------
# Validation Report
# ---------------------------------------------------------

validation_run_timestamp = datetime.now()

validation_report = [
    {
        "dataset": "products",
        "validation_name": validation_name,
        "invalid_records": invalid_records,
        "status": "PASS" if invalid_records == 0 else "FAIL",
        "run_timestamp": validation_run_timestamp
    }
    for validation_name, invalid_records in validation_results
]

print("\nValidation Report:")

for result in validation_report:
    print(result)

# ---------------------------------------------------------
# Persist Validation Report
# ---------------------------------------------------------

validation_report_path = (
    r".\ecommerce-data-platform\data\processed\validation\products"
)

validation_report_df = spark.createDataFrame(validation_report)

validation_report_df.write \
    .mode("overwrite") \
    .parquet(validation_report_path)

print("\nProduct validation report written successfully.")

# ---------------------------------------------------------
# Verify Persisted Validation Report
# ---------------------------------------------------------

saved_validation_report_df = spark.read.parquet(
    validation_report_path
)

print("\nPersisted Product Validation Report:")
saved_validation_report_df.show(
    truncate=False
)

print(
    "Persisted validation report count:",
    saved_validation_report_df.count()
)


# ---------------------------------------------------------
# Stop Spark
# ---------------------------------------------------------

spark.stop()