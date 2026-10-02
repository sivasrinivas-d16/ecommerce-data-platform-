from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col,
    count,
    countDistinct,
    sum,
    avg
)

spark = (
    SparkSession.builder
    .appName("ECommerceProductPerformance")
    .master("local[2]")
    .config("spark.driver.host", "127.0.0.1")
    .config("spark.driver.bindAddress", "127.0.0.1")
    .config("spark.hadoop.fs.permissions.umask-mode", "000")
    .getOrCreate()
)

integrated_orders_path = (
    r".\ecommerce-data-platform\data\curated\final_integrated_orders"
)

orders_df = spark.read.parquet(integrated_orders_path)

print(f"Integrated orders: {orders_df.count()}")

product_performance = (
    orders_df
    .groupBy(
        "product_id",
        "product_name",
        "category",
        "subcategory",
        "brand"
    )
    .agg(
        count("order_id").alias("total_orders"),
        countDistinct("customer_id").alias("unique_customers"),
        sum("quantity").alias("total_quantity_sold"),
        sum("order_amount").alias("gross_sales"),
        sum("net_order_amount").alias("net_revenue"),
        avg("unit_price").alias("average_selling_price")
    )
    .orderBy(
        col("net_revenue").desc()
    )
)

print("\n--- Product Performance ---")
product_performance.show(10, truncate=False)

print("\n--- Product Performance Validation ---")

original_orders = orders_df.count()

aggregated_orders = (
    product_performance
    .selectExpr("sum(total_orders) as total_orders")
    .collect()[0]["total_orders"]
)

original_quantity = (
    orders_df
    .selectExpr("sum(quantity) as total_quantity")
    .collect()[0]["total_quantity"]
)

aggregated_quantity = (
    product_performance
    .selectExpr("sum(total_quantity_sold) as total_quantity")
    .collect()[0]["total_quantity"]
)

original_gross = (
    orders_df
    .selectExpr("sum(order_amount) as gross_amount")
    .collect()[0]["gross_amount"]
)

aggregated_gross = (
    product_performance
    .selectExpr("sum(gross_sales) as gross_amount")
    .collect()[0]["gross_amount"]
)

original_net = (
    orders_df
    .selectExpr("sum(net_order_amount) as net_revenue")
    .collect()[0]["net_revenue"]
)

aggregated_net = (
    product_performance
    .selectExpr("sum(net_revenue) as net_revenue")
    .collect()[0]["net_revenue"]
)

print(f"Original order count        : {original_orders}")
print(f"Aggregated order count     : {aggregated_orders}")

print(f"Original quantity           : {original_quantity}")
print(f"Aggregated quantity         : {aggregated_quantity}")

print(f"Original gross amount       : {original_gross}")
print(f"Aggregated gross amount     : {aggregated_gross}")

print(f"Original net revenue        : {original_net}")
print(f"Aggregated net revenue      : {aggregated_net}")

product_performance_path = (
    r".\ecommerce-data-platform\data\curated\product_performance"
)

(
    product_performance
    .write
    .mode("overwrite")
    .parquet(product_performance_path)
)

print("\nProduct performance dataset written successfully.")

product_performance_df = spark.read.parquet(
    product_performance_path
)

print(
    f"Product performance rows: "
    f"{product_performance_df.count()}"
)

product_performance_df.show(10, truncate=False)

