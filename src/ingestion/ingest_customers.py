from pyspark.sql import SparkSession
from pyspark.sql.types import (
    StructType,
    StructField,
    StringType
)
from pyspark.sql.functions import col, to_date


# ---------------------------------------------------------
# Spark Session
# ---------------------------------------------------------

spark = (
    SparkSession.builder
    .appName("ECommerceCustomerIngestion")
    .master("local[2]")
    .config("spark.driver.host", "127.0.0.1")
    .config("spark.driver.bindAddress", "127.0.0.1")
    .config("spark.hadoop.fs.permissions.umask-mode", "000")
    .getOrCreate()
)


# ---------------------------------------------------------
# Customer Schema
# ---------------------------------------------------------

customer_schema = StructType([
    StructField("customer_id", StringType(), True),
    StructField("name", StringType(), True),
    StructField("email", StringType(), True),
    StructField("city", StringType(), True),
    StructField("state", StringType(), True),
    StructField("country", StringType(), True),
    StructField("signup_date", StringType(), True)
])


# ---------------------------------------------------------
# Source Path
# ---------------------------------------------------------

customers_path = r".\ecommerce-data-platform\data\customers.csv"


# ---------------------------------------------------------
# Read Customer Data
# ---------------------------------------------------------

customers_df = (
    spark.read
    .option("header", True)
    .schema(customer_schema)
    .csv(customers_path)
)

print("Customers loaded successfully")
print("Row count:", customers_df.count())

print("\nSource Schema:")
customers_df.printSchema()

print("\nSource Data:")
customers_df.show(5, truncate=False)


# ---------------------------------------------------------
# Convert signup_date
# Source format: DD-MM-YYYY
# Target type: DateType
# ---------------------------------------------------------

customers_df = customers_df.withColumn(
    "signup_date",
    to_date(col("signup_date"), "dd-MM-yyyy")
)


# ---------------------------------------------------------
# Verify Date Conversion
# ---------------------------------------------------------

print("\nSchema After Date Conversion:")
customers_df.printSchema()

print("\nData After Date Conversion:")
customers_df.show(5, truncate=False)


# ---------------------------------------------------------
# Raw Layer Path
# ---------------------------------------------------------

raw_customers_path = r".\ecommerce-data-platform\data\raw\customers"


# ---------------------------------------------------------
# Write Customers to Raw Layer
# ---------------------------------------------------------

(
    customers_df.write
    .mode("overwrite")
    .parquet(raw_customers_path)
)

print("\nCustomers written successfully to Raw layer.")


# ---------------------------------------------------------
# Verify Raw Layer
# ---------------------------------------------------------

raw_customers_df = spark.read.parquet(raw_customers_path)

print("\nRaw customer row count:", raw_customers_df.count())

print("\nRaw Customer Schema:")
raw_customers_df.printSchema()

print("\nRaw Customer Sample:")
raw_customers_df.show(5, truncate=False)


# ---------------------------------------------------------
# Stop Spark
# ---------------------------------------------------------

spark.stop()