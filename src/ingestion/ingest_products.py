import sys

from awsglue.context import GlueContext
from awsglue.job import Job
from pyspark.context import SparkContext
from pyspark.sql.types import (
    StructType,
    StructField,
    StringType,
    IntegerType,
    DecimalType,
    DateType
)


# ============================================================
# 1. Initialize Glue
# ============================================================

sc = SparkContext()
glue_context = GlueContext(sc)
spark = glue_context.spark_session

job = Job(glue_context)

args = {
    arg[2:]: sys.argv[i + 1]
    for i, arg in enumerate(sys.argv)
    if arg.startswith("--")
    and i + 1 < len(sys.argv)
    and not sys.argv[i + 1].startswith("--")
}

job_name = args.get("JOB_NAME", "ecommerce-products-etl")

job.init(job_name, {
    "JOB_NAME": job_name
})


# ============================================================
# 2. Product Schema
# ============================================================

product_schema = StructType([
    StructField("product_id", StringType(), True),
    StructField("product_name", StringType(), True),
    StructField("category", StringType(), True),
    StructField("subcategory", StringType(), True),
    StructField("brand", StringType(), True),
    StructField("price", DecimalType(12, 2), True),
    StructField("stock_quantity", IntegerType(), True),
    StructField("product_status", StringType(), True),
    StructField("created_date", DateType(), True)
])


# ============================================================
# 3. Source and Raw Output
# ============================================================

source_path = (
    "s3://ecommerce-data-platform-version1/"
    "raw/products/products.csv"
)

raw_output_path = (
    "s3://ecommerce-data-platform-version1/"
    "raw/products_parquet/"
)


# ============================================================
# 4. Read Products CSV
# ============================================================

print("Reading Products source:")
print(source_path)

products_df = (
    spark.read
    .option("header", True)
    .schema(product_schema)
    .csv(source_path)
)


# ============================================================
# 5. Basic Verification
# ============================================================

record_count = products_df.count()

print(f"Products record count: {record_count}")

if record_count == 0:
    raise RuntimeError(
        "Products ETL failed: source contains zero records."
    )


print("Products schema:")
products_df.printSchema()


# ============================================================
# 6. Write Raw Parquet
# ============================================================

print("Writing Products raw Parquet:")

(
    products_df
    .write
    .mode("overwrite")
    .parquet(raw_output_path)
)


# ============================================================
# 7. Verify Output
# ============================================================

output_df = spark.read.parquet(raw_output_path)

output_count = output_df.count()

print(f"Raw Products output count: {output_count}")

if record_count != output_count:
    raise RuntimeError(
        f"Products ETL count mismatch: "
        f"input={record_count}, output={output_count}"
    )

print("Products ETL completed successfully.")


# ============================================================
# 8. Commit Glue Job
# ============================================================

job.commit()