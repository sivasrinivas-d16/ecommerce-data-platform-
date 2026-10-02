from pyspark.sql import SparkSession
from pyspark.sql.functions import col, count, when
from datetime import datetime

spark = (
    SparkSession.builder
    .appName("ECommerceOrderValidation")
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
orders_df.printSchema()

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

# ---------------------------------------------------------
# Required Field Validation
# ---------------------------------------------------------

required_null_counts = orders_df.select(
    *[
        count(
            when(col(column_name).isNull(), 1)
        ).alias(column_name)
        for column_name in required_columns
    ]
).collect()[0]

for column_name in required_columns:
    null_count = required_null_counts[column_name]
    print(f"{column_name} null count: {null_count}")

# ---------------------------------------------------------
# Duplicate Order ID Validation
# ---------------------------------------------------------

duplicate_order_count = (
    orders_df
    .groupBy("order_id")
    .count()
    .filter(col("count") > 1)
    .count()
)

print("Duplicate order IDs:", duplicate_order_count)

# ---------------------------------------------------------
# Business Rule Validation
# ---------------------------------------------------------

business_rule_counts = orders_df.select(
    count(
        when(col("quantity") <= 0, 1)
    ).alias("invalid_quantity"),

    count(
        when(col("unit_price") < 0, 1)
    ).alias("invalid_unit_price"),

    count(
        when(col("order_amount") < 0, 1)
    ).alias("invalid_order_amount")
).collect()[0]

invalid_quantity = business_rule_counts["invalid_quantity"]
invalid_unit_price = business_rule_counts["invalid_unit_price"]
invalid_order_amount = business_rule_counts["invalid_order_amount"]

print("Invalid quantity:", invalid_quantity)
print("Invalid unit price:", invalid_unit_price)
print("Invalid order amount:", invalid_order_amount)

# ---------------------------------------------------------
# Order Amount Consistency Validation
# ---------------------------------------------------------

invalid_amount_calculations = (
    orders_df
    .filter(
        col("order_amount") !=
        (col("quantity") * col("unit_price")).cast("decimal(14,2)")
    )
    .count()
)

print("Invalid order amount calculations:", invalid_amount_calculations)


# ---------------------------------------------------------
# Status Validation
# ---------------------------------------------------------

valid_order_statuses = [
    "COMPLETED",
    "SHIPPED",
    "PROCESSING",
    "CANCELLED",
    "RETURNED"
]

valid_payment_statuses = [
    "PAID",
    "PENDING",
    "FAILED",
    "REFUNDED"
]

status_validation_counts = orders_df.select(
    count(
        when(
            ~col("order_status").isin(valid_order_statuses),
            1
        )
    ).alias("invalid_order_status"),

    count(
        when(
            ~col("payment_status").isin(valid_payment_statuses),
            1
        )
    ).alias("invalid_payment_status")
).collect()[0]

invalid_order_status_count = status_validation_counts["invalid_order_status"]
invalid_payment_status_count = status_validation_counts["invalid_payment_status"]

print("Invalid order statuses:", invalid_order_status_count)
print("Invalid payment statuses:", invalid_payment_status_count)

# ---------------------------------------------------------
# Payment Status Validation
# ---------------------------------------------------------

valid_payment_statuses = [
    "PAID",
    "PENDING",
    "FAILED",
    "REFUNDED"
]

invalid_payment_status_count = (
    orders_df
    .filter(~col("payment_status").isin(valid_payment_statuses))
    .count()
)

print("Invalid payment statuses:", invalid_payment_status_count)

# ---------------------------------------------------------
# Referential Integrity Validation
# ---------------------------------------------------------

customers_path = r".\ecommerce-data-platform\data\customers.csv"
products_path = r".\ecommerce-data-platform\data\products.csv"

customers_df = spark.read.option("header", True).csv(customers_path)
products_df = spark.read.option("header", True).csv(products_path)

print("Customers loaded:", customers_df.count())
print("Products loaded:", products_df.count())

# ---------------------------------------------------------
# Customer Referential Integrity
# ---------------------------------------------------------

invalid_customer_count = (
    orders_df
    .join(
        customers_df.select("customer_id"),
        on="customer_id",
        how="left_anti"
    )
    .count()
)

print("Orders with invalid customer IDs:", invalid_customer_count)

# ---------------------------------------------------------
# Product Referential Integrity
# ---------------------------------------------------------

invalid_product_count = (
    orders_df
    .join(
        products_df.select("product_id"),
        on="product_id",
        how="left_anti"
    )
    .count()
)

print("Orders with invalid product IDs:", invalid_product_count)

# ---------------------------------------------------------
# Reference Data Key Validation
# ---------------------------------------------------------

duplicate_customer_ids = (
    customers_df
    .groupBy("customer_id")
    .count()
    .filter(col("count") > 1)
    .count()
)

duplicate_product_ids = (
    products_df
    .groupBy("product_id")
    .count()
    .filter(col("count") > 1)
    .count()
)

print("Duplicate customer IDs:", duplicate_customer_ids)
print("Duplicate product IDs:", duplicate_product_ids)


# ---------------------------------------------------------
# Validation Summary
# ---------------------------------------------------------

validation_results = [
    ("Required Fields", 0),
    ("Duplicate Order IDs", duplicate_order_count),
    ("Invalid Quantity", invalid_quantity),
    ("Invalid Unit Price", invalid_unit_price),
    ("Invalid Order Amount", invalid_order_amount),
    ("Invalid Amount Calculations", invalid_amount_calculations),
    ("Invalid Order Status", invalid_order_status_count),
    ("Invalid Payment Status", invalid_payment_status_count),
    ("Invalid Customer IDs", invalid_customer_count),
    ("Invalid Product IDs", invalid_product_count)
]

print("\nValidation Summary")
print("-" * 60)

for validation_name, invalid_records in validation_results:

    status = "PASS" if invalid_records == 0 else "FAIL"

    print(
        f"{validation_name:<30} "
        f"{invalid_records:<10} "
        f"{status}"
    )

# ---------------------------------------------------------
# Validation Report
# ---------------------------------------------------------

validation_run_timestamp = datetime.now()

validation_report = [
    {
        "dataset": "orders",
        "validation_name": validation_name,
        "invalid_records": invalid_records,
        "status": "PASS" if invalid_records == 0 else "FAIL",
        "run_timestamp": validation_run_timestamp
    }
    for validation_name, invalid_records in validation_results
]

for result in validation_report:
    print(result)

# ---------------------------------------------------------
# Save Validation Report
# ---------------------------------------------------------

validation_report_path = r".\ecommerce-data-platform\data\processed\validation"

validation_report_df = spark.createDataFrame(validation_report)

(
    validation_report_df
    .write
    .mode("overwrite")
    .parquet(validation_report_path)
)

print("\nValidation report written successfully.")


# ---------------------------------------------------------
# Overall Validation Status
# ---------------------------------------------------------

overall_status = (
    "PASS"
    if all(invalid_records == 0 for _, invalid_records in validation_results)
    else "FAIL"
)

print("\nOverall Validation Status:", overall_status)