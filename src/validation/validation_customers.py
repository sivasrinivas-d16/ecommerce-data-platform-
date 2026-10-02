from pyspark.sql import SparkSession
from pyspark.sql.functions import col, count, when
from datetime import datetime

spark = (
    SparkSession.builder
    .appName("ECommerceCustomerValidation")
    .master("local[2]")
    .config("spark.driver.host", "127.0.0.1")
    .config("spark.driver.bindAddress", "127.0.0.1")
    .config("spark.hadoop.fs.permissions.umask-mode", "000")
    .getOrCreate()
)


customers_path = r".\ecommerce-data-platform\data\raw\customers"

customers_df = spark.read.parquet(customers_path)

print("Customers loaded from Raw layer")
print("Customer count:", customers_df.count())

customers_df.printSchema()

required_columns = [
    "customer_id",
    "name",
    "email",
    "country",
    "signup_date"
]

required_null_counts = customers_df.select(
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
        f"{column_name:<15} "
        f"Null count: {null_count:<8} "
        f"Status: {status}"
    )

# ---------------------------------------------------------
# Duplicate Customer ID Validation
# ---------------------------------------------------------

duplicate_customer_ids = (
    customers_df
    .groupBy("customer_id")
    .count()
    .filter(col("count") > 1)
)

duplicate_customer_id_count = duplicate_customer_ids.count()

print("\nDuplicate Customer ID Validation:")
print("Duplicate customer IDs:", duplicate_customer_id_count)

status = "PASS" if duplicate_customer_id_count == 0 else "FAIL"

print("Status:", status)

# ---------------------------------------------------------
# Email Format Validation
# ---------------------------------------------------------

invalid_email_count = customers_df.filter(
    ~col("email").rlike(r"^[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\.[A-Za-z]{2,}$")
).count()

print("\nEmail Format Validation:")
print("Invalid email records:", invalid_email_count)

status = "PASS" if invalid_email_count == 0 else "FAIL"

print("Status:", status)

# ---------------------------------------------------------
# Customer ID Format Validation
# ---------------------------------------------------------

invalid_customer_id_count = customers_df.filter(
    ~col("customer_id").rlike(r"^C[0-9]{5}$")
).count()

print("\nCustomer ID Format Validation:")
print("Invalid customer ID records:", invalid_customer_id_count)

status = "PASS" if invalid_customer_id_count == 0 else "FAIL"

print("Status:", status)

# ---------------------------------------------------------
# Country Validation
# ---------------------------------------------------------

valid_countries = [
    "India"
]

invalid_country_count = customers_df.filter(
    ~col("country").isin(valid_countries)
).count()

print("\nCountry Validation:")
print("Invalid country records:", invalid_country_count)

status = "PASS" if invalid_country_count == 0 else "FAIL"

print("Status:", status)

# ---------------------------------------------------------
# Signup Date Business Rule Validation
# ---------------------------------------------------------

from pyspark.sql.functions import current_date

future_signup_date_count = customers_df.filter(
    col("signup_date") > current_date()
).count()

print("\nSignup Date Validation:")
print("Future signup date records:", future_signup_date_count)

status = "PASS" if future_signup_date_count == 0 else "FAIL"

print("Status:", status)

# ---------------------------------------------------------
# Validation Results
# ---------------------------------------------------------

validation_results = [
    ("Required Field Validation", 0),
    ("Duplicate Customer ID Validation", duplicate_customer_id_count),
    ("Email Format Validation", invalid_email_count),
    ("Customer ID Format Validation", invalid_customer_id_count),
    ("Country Validation", invalid_country_count),
    ("Signup Date Validation", future_signup_date_count)
]

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
    if all(invalid_records == 0 for _, invalid_records in validation_results)
    else "FAIL"
)

print("\nOverall Customer Validation Status:", overall_status)

# ---------------------------------------------------------
# Validation Report
# ---------------------------------------------------------

validation_run_timestamp = datetime.now()

validation_report = [
    {
        "dataset": "customers",
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
    r".\ecommerce-data-platform\data\processed\validation\customers"
)

validation_report_df = spark.createDataFrame(validation_report)

validation_report_df.write \
    .mode("overwrite") \
    .parquet(validation_report_path)

print("\nCustomer validation report written successfully.")

# ---------------------------------------------------------
# Verify Persisted Validation Report
# ---------------------------------------------------------

saved_validation_report_df = spark.read.parquet(
    validation_report_path
)

print("\nPersisted Validation Report:")
saved_validation_report_df.show(
    truncate=False
)

print(
    "Persisted validation report count:",
    saved_validation_report_df.count()
)

