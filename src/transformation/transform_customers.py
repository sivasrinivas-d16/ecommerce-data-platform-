import sys

from pyspark.context import SparkContext
from pyspark.sql.functions import (
    trim,
    lower,
    col,
    year,
    month,
    dayofmonth,
    datediff,
    current_date,
    when,
    to_date
)

from awsglue.context import GlueContext
from awsglue.job import Job
from awsglue.utils import getResolvedOptions


# =========================================================
# Glue Job Initialization
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
# Read Customers from Glue Catalog
# =========================================================

customers_dyf = (
    glue_context
    .create_dynamic_frame
    .from_catalog(
        database="ecommerce_data_platform",
        table_name="customers"
    )
)

customers_df = customers_dyf.toDF()


# =========================================================
# Raw Customers
# =========================================================

print("\n--- Raw Customers ---")

customers_df.printSchema()

print(
    f"Raw customer count: "
    f"{customers_df.count()}"
)

customers_df.show(
    5,
    truncate=False
)


# =========================================================
# Convert Signup Date
# =========================================================

customers_df = customers_df.withColumn(
    "signup_date",
    to_date(
        col("signup_date"),
        "dd-MM-yyyy"
    )
)


# =========================================================
# Standardize Customer Fields
# =========================================================

customers_df = (
    customers_df
    .withColumn(
        "name",
        trim(col("name"))
    )
    .withColumn(
        "email",
        lower(trim(col("email")))
    )
    .withColumn(
        "city",
        trim(col("city"))
    )
    .withColumn(
        "state",
        trim(col("state"))
    )
    .withColumn(
        "country",
        trim(col("country"))
    )
)


print("\n--- Standardized Customers ---")

customers_df.show(
    5,
    truncate=False
)


# =========================================================
# Customer Date Transformations
# =========================================================

customers_df = (
    customers_df
    .withColumn(
        "signup_year",
        year(col("signup_date"))
    )
    .withColumn(
        "signup_month",
        month(col("signup_date"))
    )
    .withColumn(
        "signup_day",
        dayofmonth(col("signup_date"))
    )
)


print("\n--- Customer Date Transformations ---")

customers_df.show(
    5,
    truncate=False
)


# =========================================================
# Customer Tenure
# =========================================================

customers_df = customers_df.withColumn(
    "customer_tenure_days",
    datediff(
        current_date(),
        col("signup_date")
    )
)


print("\n--- Customer Tenure ---")

customers_df.select(
    "customer_id",
    "signup_date",
    "customer_tenure_days"
).show(
    5,
    truncate=False
)


# =========================================================
# Customer Status
# =========================================================

customers_df = customers_df.withColumn(
    "customer_status",
    when(
        col("customer_tenure_days") < 90,
        "NEW"
    )
    .when(
        col("customer_tenure_days") <= 365,
        "ACTIVE"
    )
    .otherwise(
        "ESTABLISHED"
    )
)


print("\n--- Customer Status ---")

customers_df.select(
    "customer_id",
    "customer_tenure_days",
    "customer_status"
).show(
    10,
    truncate=False
)


# =========================================================
# Transformed Customer Verification
# =========================================================

print(
    "\n--- Transformed Customer Verification ---"
)

print(
    f"Transformed customer count: "
    f"{customers_df.count()}"
)


print("\nCustomer Status Distribution:")

customers_df.groupBy(
    "customer_status"
).count().show()


print("\nFinal Customer Schema:")

customers_df.printSchema()


# =========================================================
# Write Curated Customers
# =========================================================

curated_customers_path = (
    "s3://ecommerce-data-platform-version1/"
    "curated/customers/"
)


(
    customers_df
    .write
    .mode("overwrite")
    .parquet(
        curated_customers_path
    )
)

print(
    "Curated customers written successfully."
)


# =========================================================
# Curated Customer Verification
# =========================================================

curated_customers_df = spark.read.parquet(
    curated_customers_path
)


print(
    "\n--- Curated Customers Verification ---"
)

print(
    f"Curated customer count: "
    f"{curated_customers_df.count()}"
)

curated_customers_df.printSchema()

curated_customers_df.show(
    5,
    truncate=False
)


# =========================================================
# Commit Glue Job
# =========================================================

job.commit()