from pyspark.sql import SparkSession
from pyspark.sql.functions import trim, lower, col
from pyspark.sql.functions import year, month, dayofmonth
from pyspark.sql.functions import datediff, current_date
from pyspark.sql.functions import when



spark = (
    SparkSession.builder
    .appName("ECommerceCustomerTransformation")
    .master("local[2]")
    .config("spark.driver.host", "127.0.0.1")
    .config("spark.driver.bindAddress", "127.0.0.1")
    .config("spark.hadoop.fs.permissions.umask-mode", "000")
    .getOrCreate()
)

raw_customers_path = r".\ecommerce-data-platform\data\raw\customers"

customers_df = spark.read.parquet(raw_customers_path)

print("\n--- Raw Customers ---")
customers_df.printSchema()

print(f"Raw customer count: {customers_df.count()}")

customers_df.show(5, truncate=False)

customers_df = (
    customers_df
    .withColumn("name", trim(col("name")))
    .withColumn("email", lower(trim(col("email"))))
    .withColumn("city", trim(col("city")))
    .withColumn("state", trim(col("state")))
    .withColumn("country", trim(col("country")))
)

print("\n--- Standardized Customers ---")
customers_df.show(5, truncate=False)

customers_df = (
    customers_df
    .withColumn("signup_year", year(col("signup_date")))
    .withColumn("signup_month", month(col("signup_date")))
    .withColumn("signup_day", dayofmonth(col("signup_date")))
)

print("\n--- Customer Date Transformations ---")
customers_df.show(5, truncate=False)

customers_df = customers_df.withColumn(
    "customer_tenure_days",
    datediff(current_date(), col("signup_date"))
)

print("\n--- Customer Tenure ---")
customers_df.select(
    "customer_id",
    "signup_date",
    "customer_tenure_days"
).show(5, truncate=False)

customers_df = customers_df.withColumn(
    "customer_status",
    when(col("customer_tenure_days") < 90, "NEW")
    .when(col("customer_tenure_days") <= 365, "ACTIVE")
    .otherwise("ESTABLISHED")
)

print("\n--- Customer Status ---")

customers_df.select(
    "customer_id",
    "customer_tenure_days",
    "customer_status"
).show(10, truncate=False)

print("\n--- Transformed Customer Verification ---")

print(f"Transformed customer count: {customers_df.count()}")

print("\nCustomer Status Distribution:")
customers_df.groupBy("customer_status").count().show()

print("\nFinal Customer Schema:")
customers_df.printSchema()

curated_customers_path = r".\ecommerce-data-platform\data\curated\customers"

(
    customers_df
    .write
    .mode("overwrite")
    .parquet(curated_customers_path)
)

print("Curated customers written successfully.")

curated_customers_df = spark.read.parquet(curated_customers_path)

print("\n--- Curated Customers Verification ---")
print(f"Curated customer count: {curated_customers_df.count()}")

curated_customers_df.printSchema()

curated_customers_df.show(5, truncate=False)

