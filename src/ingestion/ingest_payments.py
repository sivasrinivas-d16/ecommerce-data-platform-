from pyspark.sql import SparkSession
from pyspark.sql.types import (
    StructType,
    StructField,
    StringType,
    DecimalType,
    TimestampType
)


# ---------------------------------------------------------
# Spark Session
# ---------------------------------------------------------

spark = (
    SparkSession.builder
    .appName("ECommercePaymentIngestion")
    .master("local[2]")
    .config("spark.driver.host", "127.0.0.1")
    .config("spark.driver.bindAddress", "127.0.0.1")
    .config("spark.hadoop.fs.permissions.umask-mode", "000")
    .getOrCreate()
)


# ---------------------------------------------------------
# Payment Schema
# ---------------------------------------------------------

payment_schema = StructType([
    StructField("payment_id", StringType(), True),
    StructField("order_id", StringType(), True),
    StructField("customer_id", StringType(), True),
    StructField("payment_method", StringType(), True),
    StructField("payment_status", StringType(), True),
    StructField("payment_amount", DecimalType(12, 2), True),
    StructField("transaction_reference", StringType(), True),
    StructField("payment_timestamp", TimestampType(), True)
])


# ---------------------------------------------------------
# Source Path
# ---------------------------------------------------------

payments_path = r".\ecommerce-data-platform\data\payments.csv"


# ---------------------------------------------------------
# Read Payment Data
# ---------------------------------------------------------

payments_df = (
    spark.read
    .option("header", True)
    .schema(payment_schema)
    .csv(payments_path)
)

print("Payments loaded successfully")
print("Row count:", payments_df.count())

print("\nPayment Schema:")
payments_df.printSchema()

print("\nPayment Sample:")
payments_df.show(5, truncate=False)

raw_payments_path = r".\ecommerce-data-platform\data\raw\payments"

(
    payments_df.write
    .mode("overwrite")
    .parquet(raw_payments_path)
)

print("\nPayments written successfully to Raw layer.")

raw_payments_df = spark.read.parquet(raw_payments_path)

print("\nRaw payment row count:", raw_payments_df.count())

print("\nRaw Payment Schema:")
raw_payments_df.printSchema()

print("\nRaw Payment Sample:")
raw_payments_df.show(5, truncate=False)

spark.stop()


