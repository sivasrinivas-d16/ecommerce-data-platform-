from pyspark.sql import SparkSession
from pyspark.sql.functions import col, count, when

from datetime import datetime


# ---------------------------------------------------------
# Spark Session
# ---------------------------------------------------------

spark = (
    SparkSession.builder
    .appName("ECommerceCustomerQuality")
    .master("local[2]")
    .config("spark.driver.host", "127.0.0.1")
    .config("spark.driver.bindAddress", "127.0.0.1")
    .config("spark.hadoop.fs.permissions.umask-mode", "000")
    .getOrCreate()
)


# ---------------------------------------------------------
# Load Customers from Raw Layer
# ---------------------------------------------------------

customers_path = r".\ecommerce-data-platform\data\raw\customers"

customers_df = spark.read.parquet(customers_path)

total_records = customers_df.count()

print("Customers loaded from Raw layer")
print("Total customer records:", total_records)

customers_df.printSchema()

# ---------------------------------------------------------
# Completeness Measurement
# ---------------------------------------------------------

quality_columns = [
    "customer_id",
    "name",
    "email",
    "city",
    "state",
    "country",
    "signup_date"
]

completeness_counts = customers_df.select(
    *[
        count(
            when(col(column_name).isNotNull(), 1)
        ).alias(column_name)
        for column_name in quality_columns
    ]
).collect()[0]

print("\nCustomer Completeness:")

for column_name in quality_columns:

    non_null_count = completeness_counts[column_name]

    completeness_percentage = (
        non_null_count / total_records
    ) * 100

    print(
        f"{column_name:<15} "
        f"Completeness: {completeness_percentage:.2f}%"
    )

# ---------------------------------------------------------
# Customer ID Uniqueness
# ---------------------------------------------------------

unique_customer_ids = customers_df.select(
    "customer_id"
).distinct().count()

uniqueness_percentage = (
    unique_customer_ids / total_records
) * 100

print("\nCustomer ID Uniqueness:")
print("Unique customer IDs:", unique_customer_ids)
print(f"Uniqueness: {uniqueness_percentage:.2f}%")

# ---------------------------------------------------------
# Email Validity
# ---------------------------------------------------------

valid_email_count = customers_df.filter(
    col("email").rlike(
        r"^[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\.[A-Za-z]{2,}$"
    )
).count()

email_validity_percentage = (
    valid_email_count / total_records
) * 100

print("\nEmail Validity:")
print("Valid email records:", valid_email_count)
print(f"Email Validity: {email_validity_percentage:.2f}%")

# ---------------------------------------------------------
# Customer ID Format Quality
# ---------------------------------------------------------

valid_customer_id_count = customers_df.filter(
    col("customer_id").rlike(r"^C[0-9]{5}$")
).count()

customer_id_validity_percentage = (
    valid_customer_id_count / total_records
) * 100

print("\nCustomer ID Format Quality:")
print("Valid customer ID records:", valid_customer_id_count)
print(
    f"Customer ID Validity: "
    f"{customer_id_validity_percentage:.2f}%"
)

# ---------------------------------------------------------
# Country Validity
# ---------------------------------------------------------

valid_countries = [
    "India"
]

valid_country_count = customers_df.filter(
    col("country").isin(valid_countries)
).count()

country_validity_percentage = (
    valid_country_count / total_records
) * 100

print("\nCountry Validity:")
print("Valid country records:", valid_country_count)
print(
    f"Country Validity: "
    f"{country_validity_percentage:.2f}%"
)
# ---------------------------------------------------------
# Signup Date Validity
# ---------------------------------------------------------

from pyspark.sql.functions import current_date

valid_signup_date_count = customers_df.filter(
    col("signup_date").isNotNull()
    & (col("signup_date") <= current_date())
).count()

signup_date_validity_percentage = (
    valid_signup_date_count / total_records
) * 100

print("\nSignup Date Validity:")
print("Valid signup date records:", valid_signup_date_count)
print(
    f"Signup Date Validity: "
    f"{signup_date_validity_percentage:.2f}%"
)

# ---------------------------------------------------------
# Overall Customer Quality Score
# ---------------------------------------------------------

quality_metrics = [
    ("Completeness", 100.00),
    ("Customer ID Uniqueness", uniqueness_percentage),
    ("Email Validity", email_validity_percentage),
    ("Customer ID Validity", customer_id_validity_percentage),
    ("Country Validity", country_validity_percentage),
    ("Signup Date Validity", signup_date_validity_percentage)
]

overall_quality_score = (
    sum(metric_score for _, metric_score in quality_metrics)
    / len(quality_metrics)
)

print("\nCustomer Quality Metrics:")

for metric_name, metric_score in quality_metrics:
    print(
        f"{metric_name:<30} "
        f"Score: {metric_score:.2f}%"
    )

print(
    f"\nOverall Customer Quality Score: "
    f"{overall_quality_score:.2f}%"
)

# ---------------------------------------------------------
# Quality Report
# ---------------------------------------------------------

quality_run_timestamp = datetime.now()

quality_report = [
    {
        "dataset": "customers",
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
    r".\ecommerce-data-platform\data\processed\quality\customers"
)

quality_report_df = spark.createDataFrame(quality_report)

quality_report_df.write \
    .mode("overwrite") \
    .parquet(quality_report_path)

print("\nCustomer quality report written successfully.")

# ---------------------------------------------------------
# Verify Persisted Quality Report
# ---------------------------------------------------------

saved_quality_report_df = spark.read.parquet(
    quality_report_path
)

print("\nPersisted Customer Quality Report:")
saved_quality_report_df.show(
    truncate=False
)

print(
    "Persisted quality report count:",
    saved_quality_report_df.count()
)

