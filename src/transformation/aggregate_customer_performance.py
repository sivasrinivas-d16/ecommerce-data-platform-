from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col,
    count,
    countDistinct,
    sum
)

spark = (
    SparkSession.builder
    .appName("ECommerceCustomerPerformance")
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

customer_performance = (
    orders_df
    .groupBy(
        "customer_id",
        "name",
        "city",
        "state",
        "customer_status"
    )
    .agg(
        count("order_id").alias("total_orders"),
        countDistinct("product_id").alias("unique_products"),
        sum("quantity").alias("total_quantity"),
        sum("order_amount").alias("gross_sales"),
        sum("net_order_amount").alias("net_revenue")
    )
    .orderBy(
        col("net_revenue").desc()
    )
)

print("\n--- Customer Performance ---")
customer_performance.show(10, truncate=False)

print("\n--- Customer Performance Validation ---")

original_orders = orders_df.count()

aggregated_orders = (
    customer_performance
    .selectExpr("sum(total_orders) as total_orders")
    .collect()[0]["total_orders"]
)

original_quantity = (
    orders_df
    .selectExpr("sum(quantity) as total_quantity")
    .collect()[0]["total_quantity"]
)

aggregated_quantity = (
    customer_performance
    .selectExpr("sum(total_quantity) as total_quantity")
    .collect()[0]["total_quantity"]
)

original_gross = (
    orders_df
    .selectExpr("sum(order_amount) as gross_amount")
    .collect()[0]["gross_amount"]
)

aggregated_gross = (
    customer_performance
    .selectExpr("sum(gross_sales) as gross_amount")
    .collect()[0]["gross_amount"]
)

original_net = (
    orders_df
    .selectExpr("sum(net_order_amount) as net_revenue")
    .collect()[0]["net_revenue"]
)

aggregated_net = (
    customer_performance
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

customer_performance_path = (
    r".\ecommerce-data-platform\data\curated\customer_performance"
)

(
    customer_performance
    .write
    .mode("overwrite")
    .parquet(customer_performance_path)
)

print("\nCustomer performance dataset written successfully.")

customer_performance_df = spark.read.parquet(
    customer_performance_path
)

print(
    f"Customer performance rows: "
    f"{customer_performance_df.count()}"
)

customer_performance_df.show(10, truncate=False)

