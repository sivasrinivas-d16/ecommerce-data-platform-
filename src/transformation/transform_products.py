from pyspark.sql import SparkSession
from pyspark.sql.functions import trim, upper, col
from pyspark.sql.functions import when



spark = (
    SparkSession.builder
    .appName("ECommerceProductTransformation")
    .master("local[2]")
    .config("spark.driver.host", "127.0.0.1")
    .config("spark.driver.bindAddress", "127.0.0.1")
    .config("spark.hadoop.fs.permissions.umask-mode", "000")
    .getOrCreate()
)

raw_products_path = r".\ecommerce-data-platform\data\raw\products"

products_df = spark.read.parquet(raw_products_path)

print("\n--- Raw Products ---")
products_df.printSchema()

print(f"Raw product count: {products_df.count()}")

products_df.show(5, truncate=False)

products_df = (
    products_df
    .withColumn("product_name", trim(col("product_name")))
    .withColumn("category", trim(col("category")))
    .withColumn("subcategory", trim(col("subcategory")))
    .withColumn("brand", trim(col("brand")))
    .withColumn("product_status", upper(trim(col("product_status"))))
)

print("\n--- Standardized Products ---")

products_df.select(
    "product_id",
    "product_name",
    "category",
    "subcategory",
    "brand",
    "product_status"
).show(5, truncate=False)

products_df = products_df.withColumn(
    "price_category",
    when(col("price") < 1000, "LOW")
    .when(col("price") <= 10000, "MEDIUM")
    .when(col("price") <= 50000, "HIGH")
    .otherwise("PREMIUM")
)

print("\n--- Product Price Classification ---")

products_df.select(
    "product_id",
    "price",
    "price_category"
).show(10, truncate=False)

products_df = products_df.withColumn(
    "stock_status",
    when(col("stock_quantity") == 0, "OUT_OF_STOCK")
    .when(col("stock_quantity") <= 100, "LOW_STOCK")
    .when(col("stock_quantity") <= 1000, "MEDIUM_STOCK")
    .otherwise("IN_STOCK")
)

print("\n--- Product Stock Classification ---")

products_df.select(
    "product_id",
    "stock_quantity",
    "stock_status"
).show(10, truncate=False)

print("\n--- Product Transformation Verification ---")

print(f"Transformed product count: {products_df.count()}")

print("\nPrice Category Distribution:")
products_df.groupBy("price_category").count().show()

print("\nStock Status Distribution:")
products_df.groupBy("stock_status").count().show()

print("\nFinal Product Schema:")
products_df.printSchema()

curated_products_path = r".\ecommerce-data-platform\data\curated\products"

(
    products_df
    .write
    .mode("overwrite")
    .parquet(curated_products_path)
)

print("Curated products written successfully.")

curated_products_df = spark.read.parquet(curated_products_path)

print("\n--- Curated Products Verification ---")
print(f"Curated product count: {curated_products_df.count()}")

curated_products_df.printSchema()

curated_products_df.show(5, truncate=False)

