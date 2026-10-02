from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col,
    count,
    sum,
    countDistinct
)

spark = (
    SparkSession.builder
    .appName("ECommerceDailySalesAggregation")
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

daily_sales = (
    orders_df
    .groupBy("order_date")
    .agg(
        count("order_id").alias("total_orders"),
        countDistinct("customer_id").alias("unique_customers"),
        countDistinct("product_id").alias("unique_products"),
        sum("quantity").alias("total_quantity"),
        sum("order_amount").alias("gross_order_amount"),
        sum("net_order_amount").alias("net_revenue")
    )
    .orderBy("order_date")
)

print("\n--- Daily Sales ---")
daily_sales.show(10, truncate=False)

print("\n--- Daily Aggregation Validation ---")

original_orders = orders_df.count()
aggregated_orders = daily_sales.selectExpr(
    "sum(total_orders) as total_orders"
).collect()[0]["total_orders"]

original_gross = orders_df.selectExpr(
    "sum(order_amount) as gross_amount"
).collect()[0]["gross_amount"]

aggregated_gross = daily_sales.selectExpr(
    "sum(gross_order_amount) as gross_amount"
).collect()[0]["gross_amount"]

original_net = orders_df.selectExpr(
    "sum(net_order_amount) as net_revenue"
).collect()[0]["net_revenue"]

aggregated_net = daily_sales.selectExpr(
    "sum(net_revenue) as net_revenue"
).collect()[0]["net_revenue"]

print(f"Original order count       : {original_orders}")
print(f"Aggregated order count    : {aggregated_orders}")

print(f"Original gross amount     : {original_gross}")
print(f"Aggregated gross amount   : {aggregated_gross}")

print(f"Original net revenue      : {original_net}")
print(f"Aggregated net revenue    : {aggregated_net}")

daily_sales_path = (
    r".\ecommerce-data-platform\data\curated\daily_sales"
)

(
    daily_sales
    .write
    .mode("overwrite")
    .partitionBy("order_date")
    .parquet(daily_sales_path)
)

print("\nDaily sales dataset written successfully.")

daily_sales_df = spark.read.parquet(daily_sales_path)

print(
    f"Daily sales rows: "
    f"{daily_sales_df.count()}"
)

daily_sales_df.show(10, truncate=False)

