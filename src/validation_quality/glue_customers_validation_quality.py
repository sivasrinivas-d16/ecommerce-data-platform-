import sys
from datetime import datetime

from awsglue.context import GlueContext
from awsglue.job import Job
from awsglue.utils import getResolvedOptions

from pyspark.context import SparkContext
from pyspark.sql.functions import col, count, when, current_date


# =========================================================
# Glue Job Setup
# =========================================================

args = getResolvedOptions(sys.argv, ["JOB_NAME"])

sc = SparkContext()
glue_context = GlueContext(sc)
spark = glue_context.spark_session

job = Job(glue_context)
job.init(args["JOB_NAME"], args)


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
# Read RAW Glue Catalog Table
# =========================================================

print("=" * 70)
print("CUSTOMERS - VALIDATION + QUALITY")
print("=" * 70)

customers_dyf = glue_context.create_dynamic_frame.from_catalog(
    database=DATABASE_NAME,
    table_name=SOURCE_TABLE
)

customers_df = customers_dyf.toDF()

total_records = customers_df.count()

print(f"Raw customer records: {total_records}")
customers_df.printSchema()


# =========================================================
# VALIDATION
# =========================================================

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


validation_results = []


# Required fields
required_invalid_count = sum(
    required_null_counts[column_name]
    for column_name in required_columns
)

validation_results.append(
    ("Required Field Validation", required_invalid_count)
)


# Duplicate customer IDs
duplicate_customer_id_count = (
    customers_df
    .groupBy("customer_id")
    .count()
    .filter(col("count") > 1)
    .count()
)

validation_results.append(
    ("Duplicate Customer ID Validation", duplicate_customer_id_count)
)


# Email format
invalid_email_count = customers_df.filter(
    ~col("email").rlike(
        r"^[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\.[A-Za-z]{2,}$"
    )
).count()

validation_results.append(
    ("Email Format Validation", invalid_email_count)
)


# Customer ID format
invalid_customer_id_count = customers_df.filter(
    ~col("customer_id").rlike(r"^C[0-9]{5}$")
).count()

validation_results.append(
    ("Customer ID Format Validation", invalid_customer_id_count)
)


# Country
invalid_country_count = customers_df.filter(
    ~col("country").isin(["India"])
).count()

validation_results.append(
    ("Country Validation", invalid_country_count)
)


# Signup date
future_signup_date_count = customers_df.filter(
    col("signup_date") > current_date()
).count()

validation_results.append(
    ("Signup Date Validation", future_signup_date_count)
)


# =========================================================
# Overall Validation
# =========================================================

validation_status = (
    "PASS"
    if all(
        invalid_records == 0
        for _, invalid_records in validation_results
    )
    else "FAIL"
)


# =========================================================
# QUALITY
# =========================================================

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


completeness_scores = [
    (
        completeness_counts[column_name]
        / total_records
    ) * 100
    for column_name in quality_columns
]

completeness_score = (
    sum(completeness_scores)
    / len(completeness_scores)
)


unique_customer_ids = customers_df.select(
    "customer_id"
).distinct().count()

uniqueness_percentage = (
    unique_customer_ids / total_records
) * 100


valid_email_count = customers_df.filter(
    col("email").rlike(
        r"^[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\.[A-Za-z]{2,}$"
    )
).count()

email_validity_percentage = (
    valid_email_count / total_records
) * 100


valid_customer_id_count = customers_df.filter(
    col("customer_id").rlike(r"^C[0-9]{5}$")
).count()

customer_id_validity_percentage = (
    valid_customer_id_count / total_records
) * 100


valid_country_count = customers_df.filter(
    col("country").isin(["India"])
).count()

country_validity_percentage = (
    valid_country_count / total_records
) * 100


valid_signup_date_count = customers_df.filter(
    col("signup_date").isNotNull()
    & (col("signup_date") <= current_date())
).count()

signup_date_validity_percentage = (
    valid_signup_date_count / total_records
) * 100


quality_metrics = [
    ("Completeness", completeness_score),
    ("Customer ID Uniqueness", uniqueness_percentage),
    ("Email Validity", email_validity_percentage),
    ("Customer ID Validity", customer_id_validity_percentage),
    ("Country Validity", country_validity_percentage),
    ("Signup Date Validity", signup_date_validity_percentage)
]


overall_quality_score = (
    sum(score for _, score in quality_metrics)
    / len(quality_metrics)
)


# =========================================================
# Quality Gate
# =========================================================

quality_status = (
    "PASS"
    if overall_quality_score == 100.0
    else "FAIL"
)


pipeline_status = (
    "PASS"
    if validation_status == "PASS"
    and quality_status == "PASS"
    else "FAIL"
)


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


validation_report_df = spark.createDataFrame(
    validation_report
)

validation_report_df.write \
    .mode("overwrite") \
    .parquet(VALIDATION_OUTPUT)


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


quality_report_df = spark.createDataFrame(
    quality_report
)

quality_report_df.write \
    .mode("overwrite") \
    .parquet(QUALITY_OUTPUT)


# =========================================================
# Final Result
# =========================================================

print()
print("=" * 70)
print("CUSTOMER DATA QUALITY GATE")
print("=" * 70)

print(f"Validation Status : {validation_status}")
print(f"Quality Status    : {quality_status}")
print(f"Quality Score     : {overall_quality_score:.2f}%")
print(f"Pipeline Status   : {pipeline_status}")

print("=" * 70)


# =========================================================
# Fail Glue Job when Quality Gate Fails
# =========================================================

if pipeline_status != "PASS":
    raise RuntimeError(
        "Customer Validation + Quality Gate FAILED"
    )


job.commit()