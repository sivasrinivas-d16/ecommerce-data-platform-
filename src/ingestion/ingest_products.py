from pyspark.sql import SparkSession
from pyspark.sql.types import (
    StructType,
    StructField,
    StringType,
    IntegerType,
    DecimalType,
    DateType
)


# ---------------------------------------------------------
# Spark Session
# ---------------------------------------------------------

spark = (
    SparkSession.builder
    .appName("ECommerceProductIngestion")
    .master("local[2]")
    .config("spark.driver.host", "127.0.0.1")
    .config("spark.driver.bindAddress", "127.0.0.1")
    .config("spark.hadoop.fs.permissions.umask-mode", "000")
    .getOrCreate()
)


# ---------------------------------------------------------
# Product Schema
# ---------------------------------------------------------

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


# ---------------------------------------------------------
# Source Path
# ---------------------------------------------------------

products_path = r".\ecommerce-data-platform\data\products.csv"


# ---------------------------------------------------------
# Read Product Data
# ---------------------------------------------------------

products_df = (
    spark.read
    .option("header", True)
    .schema(product_schema)
    .csv(products_path)
)

print("Products loaded successfully")
print("Row count:", products_df.count())

products_df.printSchema()
products_df.show(5, truncate=False)

# ---------------------------------------------------------
# Raw Product Layer
# ---------------------------------------------------------

raw_products_path = r".\ecommerce-data-platform\data\raw\products"

(
    products_df.write
    .mode("overwrite")
    .parquet(raw_products_path)
)

print("\nProducts written successfully to Raw layer.")


# ---------------------------------------------------------
# Verify Raw Layer
# ---------------------------------------------------------

raw_products_df = spark.read.parquet(raw_products_path)

print("\nRaw product row count:", raw_products_df.count())

print("\nRaw Product Schema:")
raw_products_df.printSchema()
