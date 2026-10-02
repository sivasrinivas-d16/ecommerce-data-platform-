from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col,
    count,
    countDistinct,
    sum
)

spark = (
    SparkSession.builder
    .appName("ECommerceMonthlySalesAggregation")
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

monthly_sales = (
    orders_df
    .groupBy(
        "order_year",
        "order_month"
    )
    .agg(
        count("order_id").alias("total_orders"),
        countDistinct("customer_id").alias("unique_customers"),
        countDistinct("product_id").alias("unique_products"),
        sum("quantity").alias("total_quantity"),
        sum("order_amount").alias("gross_order_amount"),
        sum("net_order_amount").alias("net_revenue")
    )
    .orderBy(
        "order_year",
        "order_month"
    )
)

print("\n--- Monthly Sales ---")
monthly_sales.show(24, truncate=False)

print("\n--- Monthly Aggregation Validation ---")

original_orders = orders_df.count()

aggregated_orders = (
    monthly_sales
    .selectExpr("sum(total_orders) as total_orders")
    .collect()[0]["total_orders"]
)

original_gross = (
    orders_df
    .selectExpr("sum(order_amount) as gross_amount")
    .collect()[0]["gross_amount"]
)

aggregated_gross = (
    monthly_sales
    .selectExpr("sum(gross_order_amount) as gross_amount")
    .collect()[0]["gross_amount"]
)

original_net = (
    orders_df
    .selectExpr("sum(net_order_amount) as net_revenue")
    .collect()[0]["net_revenue"]
)

aggregated_net = (
    monthly_sales
    .selectExpr("sum(net_revenue) as net_revenue")
    .collect()[0]["net_revenue"]
)

print(f"Original order count       : {original_orders}")
print(f"Aggregated order count    : {aggregated_orders}")

print(f"Original gross amount     : {original_gross}")
print(f"Aggregated gross amount   : {aggregated_gross}")

print(f"Original net revenue      : {original_net}")
print(f"Aggregated net revenue    : {aggregated_net}")


monthly_sales_path = (
    r".\ecommerce-data-platform\data\curated\monthly_sales"
)

(
    monthly_sales
    .write
    .mode("overwrite")
    .partitionBy("order_year")
    .parquet(monthly_sales_path)
)

print("\nMonthly sales dataset written successfully.")

monthly_sales_df = spark.read.parquet(
    monthly_sales_path
)

print(
    f"Monthly sales rows: "
    f"{monthly_sales_df.count()}"
)

monthly_sales_df.show(24, truncate=False)

