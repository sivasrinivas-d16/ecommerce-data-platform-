import sys
from datetime import datetime

from awsglue.context import GlueContext
from awsglue.job import Job
from awsglue.utils import getResolvedOptions

from pyspark.context import SparkContext
from pyspark.sql.functions import (
    col,
    count,
    when,
    current_date,
    to_date
)


# =========================================================
# Glue Job Setup
# =========================================================

args = getResolvedOptions(
    sys.argv,
    ["JOB_NAME"]
)

sc = SparkContext()

glue_context = GlueContext(sc)

spark = glue_context.spark_session

job = Job(glue_context)

job.init(
    args["JOB_NAME"],
    args
)


# =========================================================
# Configuration
# =========================================================

DATABASE_NAME = "ecommerce_data_platform"

SOURCE_TABLE = "customers"

VALIDATION_OUTPUT = (
    "s3://ecommerce-data-platform-version1/"
    "processed/validation/customers/"
)

QUALITY_OUTPUT = (
    "s3://ecommerce-data-platform-version1/"
    "processed/quality/customers/"
)


# =========================================================
# Load RAW Customers from Glue Catalog
# =========================================================

print("=" * 70)
print("CUSTOMERS - VALIDATION + QUALITY")
print("=" * 70)

print(
    f"Glue Database : {DATABASE_NAME}"
)

print(
    f"Glue Table    : {SOURCE_TABLE}"
)


customers_dyf = (
    glue_context
    .create_dynamic_frame
    .from_catalog(
        database=DATABASE_NAME,
        table_name=SOURCE_TABLE
    )
)

customers_df = customers_dyf.toDF()


# =========================================================
# Normalize RAW Signup Date
# =========================================================
#
# Raw CSV stores signup_date as DD-MM-YYYY.
# Glue Catalog intentionally registers the raw field
# as string so that the raw physical representation
# is preserved.
#
# Convert it only for validation/quality processing.
# =========================================================

customers_df = customers_df.withColumn(
    "signup_date",
    to_date(
        col("signup_date"),
        "dd-MM-yyyy"
    )
)


# =========================================================
# Customer Count
# =========================================================

total_records = customers_df.count()

print(
    f"Total customer records: {total_records}"
)

customers_df.printSchema()


# =========================================================
# VALIDATION
# =========================================================

print()
print("=" * 70)
print("VALIDATION")
print("=" * 70)


# ---------------------------------------------------------
# Required Field Validation
# ---------------------------------------------------------

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
            when(
                col(column_name).isNull(),
                1
            )
        ).alias(column_name)
        for column_name in required_columns
    ]
).collect()[0]


required_invalid_count = sum(
    required_null_counts[column_name]
    for column_name in required_columns
)


# ---------------------------------------------------------
# Duplicate Customer ID Validation
# ---------------------------------------------------------

duplicate_customer_id_count = (
    customers_df
    .groupBy("customer_id")
    .count()
    .filter(
        col("count") > 1
    )
    .count()
)


# ---------------------------------------------------------
# Email Format Validation
# ---------------------------------------------------------

invalid_email_count = customers_df.filter(
    ~col("email").rlike(
        r"^[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\.[A-Za-z]{2,}$"
    )
).count()


# ---------------------------------------------------------
# Customer ID Format Validation
# ---------------------------------------------------------

invalid_customer_id_count = customers_df.filter(
    ~col("customer_id").rlike(
        r"^C[0-9]{5}$"
    )
).count()


# ---------------------------------------------------------
# Country Validation
# ---------------------------------------------------------

valid_countries = [
    "India"
]


invalid_country_count = customers_df.filter(
    ~col("country").isin(
        valid_countries
    )
).count()


# ---------------------------------------------------------
# Signup Date Business Rule Validation
# ---------------------------------------------------------

future_signup_date_count = customers_df.filter(
    col("signup_date") > current_date()
).count()


# =========================================================
# Validation Results
# =========================================================

validation_results = [
    (
        "Required Field Validation",
        required_invalid_count
    ),
    (
        "Duplicate Customer ID Validation",
        duplicate_customer_id_count
    ),
    (
        "Email Format Validation",
        invalid_email_count
    ),
    (
        "Customer ID Format Validation",
        invalid_customer_id_count
    ),
    (
        "Country Validation",
        invalid_country_count
    ),
    (
        "Signup Date Validation",
        future_signup_date_count
    )
]


# =========================================================
# Print Validation Results
# =========================================================

print()

for validation_name, invalid_records in validation_results:

    status = (
        "PASS"
        if invalid_records == 0
        else "FAIL"
    )

    print(
        f"{validation_name:<40}"
        f"Invalid Records: {invalid_records:<8}"
        f"Status: {status}"
    )


# =========================================================
# Overall Validation Status
# =========================================================

validation_status = (
    "PASS"
    if all(
        invalid_records == 0
        for _, invalid_records
        in validation_results
    )
    else "FAIL"
)


print()
print(
    "Overall Validation Status:",
    validation_status
)


# =========================================================
# QUALITY
# =========================================================

print()
print("=" * 70)
print("QUALITY")
print("=" * 70)


# ---------------------------------------------------------
# Quality Columns
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


# ---------------------------------------------------------
# Completeness
# ---------------------------------------------------------

completeness_counts = customers_df.select(
    *[
        count(
            when(
                col(column_name).isNotNull(),
                1
            )
        ).alias(column_name)
        for column_name in quality_columns
    ]
).collect()[0]


completeness_scores = []

for column_name in quality_columns:

    non_null_count = (
        completeness_counts[column_name]
    )

    completeness_percentage = (
        non_null_count / total_records
    ) * 100

    completeness_scores.append(
        completeness_percentage
    )

    print(
        f"{column_name:<15}"
        f"Completeness: "
        f"{completeness_percentage:.2f}%"
    )


# =========================================================
# Overall Completeness Score
# =========================================================

completeness_score = (
    sum(completeness_scores)
    / len(completeness_scores)
)


# ---------------------------------------------------------
# Customer ID Uniqueness
# ---------------------------------------------------------

unique_customer_ids = (
    customers_df
    .select("customer_id")
    .distinct()
    .count()
)


uniqueness_percentage = (
    unique_customer_ids
    / total_records
) * 100


# ---------------------------------------------------------
# Email Validity
# ---------------------------------------------------------

valid_email_count = customers_df.filter(
    col("email").rlike(
        r"^[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\.[A-Za-z]{2,}$"
    )
).count()


email_validity_percentage = (
    valid_email_count
    / total_records
) * 100


# ---------------------------------------------------------
# Customer ID Format Quality
# ---------------------------------------------------------

valid_customer_id_count = customers_df.filter(
    col("customer_id").rlike(
        r"^C[0-9]{5}$"
    )
).count()


customer_id_validity_percentage = (
    valid_customer_id_count
    / total_records
) * 100


# ---------------------------------------------------------
# Country Validity
# ---------------------------------------------------------

valid_country_count = customers_df.filter(
    col("country").isin(
        valid_countries
    )
).count()


country_validity_percentage = (
    valid_country_count
    / total_records
) * 100


# ---------------------------------------------------------
# Signup Date Validity
# ---------------------------------------------------------

valid_signup_date_count = customers_df.filter(
    col("signup_date").isNotNull()
    & (
        col("signup_date")
        <= current_date()
    )
).count()


signup_date_validity_percentage = (
    valid_signup_date_count
    / total_records
) * 100


# =========================================================
# Quality Metrics
# =========================================================

quality_metrics = [
    (
        "Completeness",
        completeness_score
    ),
    (
        "Customer ID Uniqueness",
        uniqueness_percentage
    ),
    (
        "Email Validity",
        email_validity_percentage
    ),
    (
        "Customer ID Validity",
        customer_id_validity_percentage
    ),
    (
        "Country Validity",
        country_validity_percentage
    ),
    (
        "Signup Date Validity",
        signup_date_validity_percentage
    )
]


# =========================================================
# Print Quality Metrics
# =========================================================

print()

for metric_name, metric_score in quality_metrics:

    print(
        f"{metric_name:<30}"
        f"Score: {metric_score:.2f}%"
    )


# =========================================================
# Overall Quality Score
# =========================================================

overall_quality_score = (
    sum(
        metric_score
        for _, metric_score
        in quality_metrics
    )
    / len(quality_metrics)
)


print()

print(
    f"Overall Customer Quality Score: "
    f"{overall_quality_score:.2f}%"
)


# =========================================================
# Quality Status
# =========================================================

quality_status = (
    "PASS"
    if overall_quality_score == 100.0
    else "FAIL"
)


# =========================================================
# Final Pipeline Status
# =========================================================

pipeline_status = (
    "PASS"
    if (
        validation_status == "PASS"
        and quality_status == "PASS"
    )
    else "FAIL"
)


# =========================================================
# Print Final Gate
# =========================================================

print()
print("=" * 70)
print("CUSTOMER DATA QUALITY GATE")
print("=" * 70)

print(
    f"Validation Status : {validation_status}"
)

print(
    f"Quality Status    : {quality_status}"
)

print(
    f"Quality Score     : "
    f"{overall_quality_score:.2f}%"
)

print(
    f"Pipeline Status   : {pipeline_status}"
)

print("=" * 70)


# =========================================================
# Validation Report
# =========================================================

run_timestamp = datetime.now()


validation_report = [
    {
        "dataset": "customers",
        "validation_name": validation_name,
        "invalid_records": invalid_records,
        "status": (
            "PASS"
            if invalid_records == 0
            else "FAIL"
        ),
        "run_timestamp": run_timestamp
    }
    for validation_name, invalid_records
    in validation_results
]


validation_report_df = (
    spark.createDataFrame(
        validation_report
    )
)


validation_report_df.write \
    .mode("overwrite") \
    .parquet(
        VALIDATION_OUTPUT
    )


print(
    "\nCustomer validation report "
    "written successfully."
)


# =========================================================
# Quality Report
# =========================================================

quality_report = [
    {
        "dataset": "customers",
        "metric_name": metric_name,
        "metric_score": float(metric_score),
        "run_timestamp": run_timestamp
    }
    for metric_name, metric_score
    in quality_metrics
]


quality_report_df = (
    spark.createDataFrame(
        quality_report
    )
)


quality_report_df.write \
    .mode("overwrite") \
    .parquet(
        QUALITY_OUTPUT
    )


print(
    "Customer quality report "
    "written successfully."
)


# =========================================================
# Verify Reports
# =========================================================

print()
print("=" * 70)
print("REPORT VERIFICATION")
print("=" * 70)


saved_validation_report_df = (
    spark.read.parquet(
        VALIDATION_OUTPUT
    )
)


print(
    "Validation report count:",
    saved_validation_report_df.count()
)


saved_quality_report_df = (
    spark.read.parquet(
        QUALITY_OUTPUT
    )
)


print(
    "Quality report count:",
    saved_quality_report_df.count()
)


# =========================================================
# Fail Glue Job if Gate Fails
# =========================================================

if pipeline_status != "PASS":

    raise RuntimeError(
        "Customer Validation + Quality Gate FAILED"
    )


# =========================================================
# Commit Glue Job
# =========================================================

job.commit()