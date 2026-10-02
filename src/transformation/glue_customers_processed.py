import sys

from awsglue.context import GlueContext
from awsglue.job import Job
from awsglue.utils import getResolvedOptions

from pyspark.context import SparkContext
from pyspark.sql.functions import (
    col,
    lower,
    trim,
    to_date,
    year,
    month,
    dayofmonth,
    when,
)


# ---------------------------------------------------------
# Glue Job Setup
# ---------------------------------------------------------

args = getResolvedOptions(
    sys.argv,
    ["JOB_NAME"]
)

sc = SparkContext()
glue_context = GlueContext(sc)
spark = glue_context.spark_session

job = Job(glue_context)
job.init(args["JOB_NAME"], args)


# ---------------------------------------------------------
# Configuration
# ---------------------------------------------------------

DATABASE_NAME = "ecommerce_data_platform"
SOURCE_TABLE = "customers"

OUTPUT_PATH = (
    "s3://ecommerce-data-platform-version1/"
    "processed/customers/"
)


# ---------------------------------------------------------
# Read RAW table from Glue Data Catalog
# ---------------------------------------------------------

print("=" * 70)
print("CUSTOMERS RAW → PROCESSED")
print("=" * 70)

print(f"Database     : {DATABASE_NAME}")
print(f"Source table : {SOURCE_TABLE}")
print(f"Output path  : {OUTPUT_PATH}")

customers_dyf = glue_context.create_dynamic_frame.from_catalog(
    database=DATABASE_NAME,
    table_name=SOURCE_TABLE
)

customers_df = customers_dyf.toDF()

print(f"Raw record count: {customers_df.count()}")


# ---------------------------------------------------------
# Data Type Conversion
# ---------------------------------------------------------

customers_df = customers_df.withColumn(
    "signup_date",
    to_date(col("signup_date"), "dd-MM-yyyy")
)


# ---------------------------------------------------------
# Data Cleaning
# ---------------------------------------------------------

customers_df = (
    customers_df
    .withColumn("customer_id", trim(col("customer_id")))
    .withColumn("name", trim(col("name")))
    .withColumn("email", lower(trim(col("email"))))
    .withColumn("city", trim(col("city")))
    .withColumn("state", trim(col("state")))
    .withColumn("country", trim(col("country")))
)


# ---------------------------------------------------------
# Validation Flags
# ---------------------------------------------------------

customers_df = (
    customers_df
    .withColumn(
        "required_fields_valid",
        (
            col("customer_id").isNotNull()
            & col("name").isNotNull()
            & col("email").isNotNull()
            & col("country").isNotNull()
            & col("signup_date").isNotNull()
        )
    )
    .withColumn(
        "customer_id_valid",
        col("customer_id").rlike("^C[0-9]{5}$")
    )
    .withColumn(
        "email_valid",
        col("email").rlike(
            r"^[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\.[A-Za-z]{2,}$"
        )
    )
    .withColumn(
        "country_valid",
        col("country") == "India"
    )
)


# ---------------------------------------------------------
# Processed Layer Filter
# ---------------------------------------------------------

processed_df = customers_df.filter(
    col("required_fields_valid")
    & col("customer_id_valid")
    & col("email_valid")
    & col("country_valid")
)


# ---------------------------------------------------------
# Add Processing Metadata
# ---------------------------------------------------------

processed_df = (
    processed_df
    .withColumn("signup_year", year(col("signup_date")))
    .withColumn("signup_month", month(col("signup_date")))
    .withColumn("signup_day", dayofmonth(col("signup_date")))
    .withColumn(
        "processing_status",
        when(
            col("required_fields_valid")
            & col("customer_id_valid")
            & col("email_valid")
            & col("country_valid"),
            "VALID"
        ).otherwise("INVALID")
    )
)


# ---------------------------------------------------------
# Processing Metrics
# ---------------------------------------------------------

raw_count = customers_df.count()
processed_count = processed_df.count()

invalid_count = raw_count - processed_count

print()
print("-" * 70)
print("PROCESSING SUMMARY")
print("-" * 70)

print(f"Raw records       : {raw_count}")
print(f"Processed records : {processed_count}")
print(f"Invalid records   : {invalid_count}")


# ---------------------------------------------------------
# Write PROCESSED Layer
# ---------------------------------------------------------

(
    processed_df
    .write
    .mode("overwrite")
    .format("parquet")
    .save(OUTPUT_PATH)
)


# ---------------------------------------------------------
# Final Verification
# ---------------------------------------------------------

print()
print("-" * 70)
print("OUTPUT VERIFICATION")
print("-" * 70)

output_df = spark.read.parquet(OUTPUT_PATH)

print(f"Processed output count: {output_df.count()}")

output_df.show(10, truncate=False)

print()
print("=" * 70)
print("CUSTOMERS RAW → PROCESSED COMPLETED")
print("=" * 70)


job.commit()