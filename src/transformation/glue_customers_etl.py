import sys

from awsglue.context import GlueContext
from awsglue.job import Job
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from pyspark.sql.functions import (
    col, lower, trim, to_date, year, month, dayofmonth,
    datediff, current_date, when
)

args = getResolvedOptions(sys.argv, ["JOB_NAME"])

sc = SparkContext()
glue_context = GlueContext(sc)
spark = glue_context.spark_session

job = Job(glue_context)
job.init(args["JOB_NAME"], args)

INPUT_PATH = "s3://ecommerce-data-platform-version1/raw/customers/customers.csv"
OUTPUT_PATH = "s3://ecommerce-data-platform-version1/curated/customers/"

customers_df = (
    spark.read
    .option("header", "true")
    .option("inferSchema", "false")
    .csv(INPUT_PATH)
)

customers_transformed = (
    customers_df
    .withColumn("customer_id", trim(col("customer_id")))
    .withColumn("name", trim(col("name")))
    .withColumn("email", lower(trim(col("email"))))
    .withColumn("city", trim(col("city")))
    .withColumn("state", trim(col("state")))
    .withColumn("country", trim(col("country")))
    .withColumn("signup_date", to_date(col("signup_date"), "dd-MM-yyyy"))
    .withColumn("signup_year", year(col("signup_date")))
    .withColumn("signup_month", month(col("signup_date")))
    .withColumn("signup_day", dayofmonth(col("signup_date")))
    .withColumn("tenure_days", datediff(current_date(), col("signup_date")))
    .withColumn(
        "customer_status",
        when(col("tenure_days") < 90, "NEW")
        .when(col("tenure_days") <= 365, "ACTIVE")
        .otherwise("ESTABLISHED")
    )
)

(
    customers_transformed
    .write
    .mode("overwrite")
    .format("parquet")
    .save(OUTPUT_PATH)
)

print("=" * 60)
print("Glue Customers ETL completed successfully")
print("=" * 60)
print(f"Input  : {INPUT_PATH}")
print(f"Output : {OUTPUT_PATH}")
print(f"Records: {customers_transformed.count()}")
print("=" * 60)

job.commit()
