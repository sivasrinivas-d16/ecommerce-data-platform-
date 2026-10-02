from pyspark.sql import SparkSession
from pyspark.sql.functions import trim, upper, col
from pyspark.sql.functions import to_date, year, month, dayofmonth
from pyspark.sql.functions import when



spark = (
    SparkSession.builder
    .appName("ECommercePaymentTransformation")
    .master("local[2]")
    .config("spark.driver.host", "127.0.0.1")
    .config("spark.driver.bindAddress", "127.0.0.1")
    .config("spark.hadoop.fs.permissions.umask-mode", "000")
    .getOrCreate()
)

raw_payments_path = r".\ecommerce-data-platform\data\raw\payments"

payments_df = spark.read.parquet(raw_payments_path)

print("\n--- Raw Payments ---")
payments_df.printSchema()

print(f"Raw payment count: {payments_df.count()}")

payments_df.show(5, truncate=False)

payments_df = (
    payments_df
    .withColumn("payment_method", upper(trim(col("payment_method"))))
    .withColumn("payment_status", upper(trim(col("payment_status"))))
    .withColumn("transaction_reference", trim(col("transaction_reference")))
)

print("\n--- Standardized Payments ---")

payments_df.select(
    "payment_id",
    "payment_method",
    "payment_status",
    "transaction_reference"
).show(10, truncate=False)

payments_df = (
    payments_df
    .withColumn("payment_date", to_date(col("payment_timestamp")))
    .withColumn("payment_year", year(col("payment_timestamp")))
    .withColumn("payment_month", month(col("payment_timestamp")))
    .withColumn("payment_day", dayofmonth(col("payment_timestamp")))
)

print("\n--- Payment Date Transformations ---")

payments_df.select(
    "payment_id",
    "payment_timestamp",
    "payment_date",
    "payment_year",
    "payment_month",
    "payment_day"
).show(10, truncate=False)

payments_df = payments_df.withColumn(
    "payment_value_category",
    when(col("payment_amount") < 1000, "LOW_VALUE")
    .when(col("payment_amount") <= 10000, "MEDIUM_VALUE")
    .when(col("payment_amount") <= 50000, "HIGH_VALUE")
    .otherwise("PREMIUM_VALUE")
)

print("\n--- Payment Value Classification ---")

payments_df.select(
    "payment_id",
    "payment_amount",
    "payment_value_category"
).show(10, truncate=False)

payments_df = payments_df.withColumn(
    "payment_result",
    when(col("payment_status") == "PAID", "SUCCESS")
    .when(col("payment_status") == "FAILED", "FAILED")
    .when(col("payment_status") == "PENDING", "PENDING")
    .when(col("payment_status") == "REFUNDED", "REFUNDED")
    .otherwise("UNKNOWN")
)

print("\n--- Payment Result ---")

payments_df.select(
    "payment_id",
    "payment_status",
    "payment_result"
).show(10, truncate=False)

print("\n--- Payment Transformation Verification ---")

print(f"Transformed payment count: {payments_df.count()}")

print("\nPayment Value Category Distribution:")
payments_df.groupBy("payment_value_category").count().show()

print("\nPayment Result Distribution:")
payments_df.groupBy("payment_result").count().show()

print("\nFinal Payment Schema:")
payments_df.printSchema()

print(f"Transformed payment count: {payments_df.count()}")

print("\nPayment Value Category Distribution:")
payments_df.groupBy("payment_value_category").count().show()

print("\nPayment Result Distribution:")
payments_df.groupBy("payment_result").count().show()

curated_payments_path = r".\ecommerce-data-platform\data\curated\payments"

(
    payments_df
    .write
    .mode("overwrite")
    .parquet(curated_payments_path)
)

print("Curated payments written successfully.")

curated_payments_df = spark.read.parquet(curated_payments_path)

print("\n--- Curated Payments Verification ---")
print(f"Curated payment count: {curated_payments_df.count()}")

curated_payments_df.printSchema()

curated_payments_df.show(5, truncate=False)

