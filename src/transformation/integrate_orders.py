from pyspark.sql import SparkSession
from pyspark.sql.functions import col
from pyspark.sql.functions import when
from pyspark.sql.functions import abs, round, avg, min, max


spark = (
    SparkSession.builder
    .appName("ECommerceOrderIntegration")
    .master("local[2]")
    .config("spark.driver.host", "127.0.0.1")
    .config("spark.driver.bindAddress", "127.0.0.1")
    .config("spark.hadoop.fs.permissions.umask-mode", "000")
    .getOrCreate()
)

orders_path = r".\ecommerce-data-platform\data\curated\orders"
customers_path = r".\ecommerce-data-platform\data\curated\customers"
products_path = r".\ecommerce-data-platform\data\curated\products"

orders_df = spark.read.parquet(orders_path)
customers_df = spark.read.parquet(customers_path)
products_df = spark.read.parquet(products_path)

print("\n--- Dataset Counts ---")
print(f"Orders: {orders_df.count()}")
print(f"Customers: {customers_df.count()}")
print(f"Products: {products_df.count()}")

customer_lookup = customers_df.select(
    "customer_id",
    "name",
    "city",
    "state",
    "country",
    "customer_status"
)

product_lookup = products_df.select(
    "product_id",
    "product_name",
    "category",
    "subcategory",
    "brand",
    "price",
    "price_category",
    "stock_status"
)

print("\n--- Customer Lookup ---")
customer_lookup.show(5, truncate=False)

print("\n--- Product Lookup ---")
product_lookup.show(5, truncate=False)

orders_with_customers = (
    orders_df
    .join(
        customer_lookup,
        on="customer_id",
        how="left"
    )
)

print("\n--- Orders + Customers ---")

print(
    f"Orders after customer join: "
    f"{orders_with_customers.count()}"
)

orders_with_customers.show(5, truncate=False)

orders_enriched = (
    orders_with_customers
    .join(
        product_lookup,
        on="product_id",
        how="left"
    )
)

print("\n--- Orders + Customers + Products ---")

print(
    f"Orders after product join: "
    f"{orders_enriched.count()}"
)

orders_enriched.show(5, truncate=False)

print("\n--- Integration Validation ---")

total_orders = orders_enriched.count()

missing_customers = (
    orders_enriched
    .filter(col("name").isNull())
    .count()
)

missing_products = (
    orders_enriched
    .filter(col("product_name").isNull())
    .count()
)

duplicate_orders = (
    orders_enriched
    .groupBy("order_id")
    .count()
    .filter(col("count") > 1)
    .count()
)

print(f"Total orders          : {total_orders}")
print(f"Missing customers     : {missing_customers}")
print(f"Missing products      : {missing_products}")
print(f"Duplicate order IDs   : {duplicate_orders}")

integrated_orders_path = r".\ecommerce-data-platform\data\curated\integrated_orders"

(
    orders_enriched
    .write
    .mode("overwrite")
    .partitionBy("order_year", "order_month")
    .parquet(integrated_orders_path)
)

print("\nIntegrated orders written successfully.")

integrated_orders_df = spark.read.parquet(integrated_orders_path)

print(
    f"Integrated orders row count: "
    f"{integrated_orders_df.count()}"
)

integrated_orders_df.printSchema()

payments_df = spark.read.parquet(
    r".\ecommerce-data-platform\data\raw\payments"
)

payment_lookup = payments_df.select(
    "payment_id",
    "order_id",
    "payment_method",
    col("payment_status").alias("actual_payment_status"),
    "payment_amount",
    "transaction_reference",
    "payment_timestamp"
)

print("\n--- Payment Lookup ---")
payment_lookup.show(5, truncate=False)

final_integrated_orders = (
    orders_enriched
    .join(
        payment_lookup,
        on="order_id",
        how="left"
    )
)

print("\n--- Orders + Customers + Products + Payments ---")

print(
    f"Orders after payment join: "
    f"{final_integrated_orders.count()}"
)

final_integrated_orders.show(5, truncate=False)

from pyspark.sql.functions import when

final_integrated_orders = (
    final_integrated_orders
    .withColumn(
        "payment_status_reconciliation",
        when(
            col("payment_id").isNull(),
            "NO_PAYMENT"
        )
        .when(
            col("payment_status") == col("actual_payment_status"),
            "MATCH"
        )
        .otherwise("MISMATCH")
    )
)

print("\n--- Payment Reconciliation ---")

final_integrated_orders.groupBy(
    "payment_status_reconciliation"
).count().show()

final_integrated_orders = (
    final_integrated_orders
    .withColumn(
        "payment_amount_reconciliation",
        when(
            col("payment_id").isNull(),
            "NO_PAYMENT"
        )
        .when(
            col("payment_amount") == col("order_amount"),
            "MATCH"
        )
        .otherwise("MISMATCH")
    )
)

print("\n--- Payment Amount Reconciliation ---")

final_integrated_orders.groupBy(
    "payment_amount_reconciliation"
).count().show()


payment_difference_summary = (
    final_integrated_orders
    .withColumn(
        "payment_order_difference",
        round(abs(col("payment_amount") - col("order_amount")), 2)
    )
    .select(
        round(avg("payment_order_difference"), 2).alias("avg_difference"),
        round(min("payment_order_difference"), 2).alias("min_difference"),
        round(max("payment_order_difference"), 2).alias("max_difference")
    )
)

print("\n--- Payment vs Order Amount Difference ---")
payment_difference_summary.show()

final_integrated_orders = (
    final_integrated_orders
    .withColumn(
        "payment_order_difference",
        round(
            col("payment_amount") - col("order_amount"),
            2
        )
    )
)

print("\n--- Final Payment Reconciliation Sample ---")

final_integrated_orders.select(
    "order_id",
    "order_amount",
    "payment_amount",
    "payment_order_difference",
    "payment_amount_reconciliation"
).show(10, truncate=False)

print("\n--- Final Integration Validation ---")

total_orders = final_integrated_orders.count()

missing_customers = (
    final_integrated_orders
    .filter(col("name").isNull())
    .count()
)

missing_products = (
    final_integrated_orders
    .filter(col("product_name").isNull())
    .count()
)

missing_payments = (
    final_integrated_orders
    .filter(col("payment_id").isNull())
    .count()
)

duplicate_orders = (
    final_integrated_orders
    .groupBy("order_id")
    .count()
    .filter(col("count") > 1)
    .count()
)

print(f"Total orders        : {total_orders}")
print(f"Missing customers   : {missing_customers}")
print(f"Missing products    : {missing_products}")
print(f"Missing payments    : {missing_payments}")
print(f"Duplicate order IDs : {duplicate_orders}")

final_integrated_orders_path = (
    r".\ecommerce-data-platform\data\curated\final_integrated_orders"
)

(
    final_integrated_orders
    .write
    .mode("overwrite")
    .partitionBy("order_year", "order_month")
    .parquet(final_integrated_orders_path)
)

print("\nFinal integrated orders written successfully.")

final_integrated_df = spark.read.parquet(
    final_integrated_orders_path
)

print(
    f"Final integrated row count: "
    f"{final_integrated_df.count()}"
)

final_integrated_df.printSchema()

