
import sys
from datetime import datetime

from awsglue.utils import getResolvedOptions
from awsglue.context import GlueContext
from awsglue.job import Job

from pyspark.context import SparkContext
from pyspark.sql import functions as F
from pyspark.sql.types import (
    StructType, StructField, StringType, IntegerType,
    DecimalType, TimestampType, DateType
)

# ============================================================
# 1. CONFIGURATION
# ============================================================

args = getResolvedOptions(sys.argv, ["JOB_NAME"])

BUCKET = "ecommerce-data-platform-version1"
OUTPUT_PREFIX = f"s3://{BUCKET}/data"

N_CUSTOMERS = 10_000
N_PRODUCTS = 100_000
N_ORDERS = 1_000_000
N_PAYMENTS = 1_000_000
N_EVENTS = 5_000_000

# Tune these according to your Glue worker capacity.
OUTPUT_PARTITIONS = 8
SEED = 42

# ============================================================
# 2. INITIALIZE GLUE / SPARK
# ============================================================

sc = SparkContext.getOrCreate()
glue_context = GlueContext(sc)
spark = glue_context.spark_session

job = Job(glue_context)
job.init(args["JOB_NAME"], args)

spark.conf.set("spark.sql.session.timeZone", "UTC")

print("Source data generation started")
print("S3 destination:", OUTPUT_PREFIX)

# ============================================================
# 3. REUSABLE HELPERS
# ============================================================

def random_timestamp(start, end, seed):
    """Generate timestamps uniformly within a date range."""
    start_epoch = int(
        datetime.strptime(start, "%Y-%m-%d").timestamp()
    )
    end_epoch = int(
        datetime.strptime(end, "%Y-%m-%d").timestamp()
    )

    seconds = end_epoch - start_epoch

    return F.to_timestamp(
        F.from_unixtime(
            F.lit(start_epoch)
            + F.floor(F.rand(seed) * seconds).cast("long")
        )
    )


def write_csv(df, dataset_name):
    """Write a Spark DataFrame to an S3 CSV prefix."""
    destination = f"{OUTPUT_PREFIX}/{dataset_name}.csv"

    (
        df.repartition(OUTPUT_PARTITIONS)
        .write
        .mode("overwrite")
        .option("header", True)
        .option("maxRecordsPerFile", 250_000)
        .csv(destination)
    )

    print(f"Written dataset: {destination}")


def random_choice(seed, choices, probabilities=None):
    """Choose a value using a Spark random number."""
    r = F.rand(seed)

    if probabilities is None:
        return F.element_at(
            F.array(*[F.lit(x) for x in choices]),
            (F.floor(r * len(choices)) + 1).cast("int")
        )

    result = F.lit(choices[-1])
    cumulative = 1.0

    for i in range(len(choices) - 1, -1, -1):
        if i == len(choices) - 1:
            cumulative = sum(probabilities[:i + 1])
            result = F.when(r < cumulative, F.lit(choices[i]))
        else:
            cumulative = sum(probabilities[:i + 1])
            result = F.when(r < cumulative, F.lit(choices[i])).otherwise(result)

    return result.otherwise(F.lit(choices[0]))


# ============================================================
# 4. CUSTOMERS — 10,000 ROWS
# ============================================================

customers = (
    spark.range(1, N_CUSTOMERS + 1)
    .select(
        F.format_string("C%05d", F.col("id")).alias("customer_id"),
        F.concat(F.lit("Customer "), F.col("id")).alias("name"),
        F.concat(
            F.lit("customer"),
            F.col("id"),
            F.lit("@example.com")
        ).alias("email"),
        random_choice(101, ["Sydney", "Melbourne", "Brisbane",
                            "Perth", "Adelaide"]).alias("city"),
        random_choice(102, ["NSW", "VIC", "QLD", "WA", "SA"]).alias("state"),
        F.lit("Australia").alias("country"),
        F.date_format(
            random_timestamp("2022-01-01", "2025-01-01", 103),
            "dd-MM-yyyy"
        ).alias("signup_date")
    )
)

write_csv(customers, "customers")

# ============================================================
# 5. PRODUCTS — 100,000 ROWS
# ============================================================

products = (
    spark.range(1, N_PRODUCTS + 1)
    .select(
        F.format_string("P%06d", F.col("id")).alias("product_id"),
        F.concat(F.lit("Product "), F.col("id")).alias("product_name"),
        random_choice(201, ["Electronics", "Home", "Fashion",
                            "Beauty", "Sports"]).alias("category"),
        random_choice(202, ["Standard", "Premium", "Basic"])
            .alias("subcategory"),
        random_choice(203, ["BrandA", "BrandB", "BrandC"])
            .alias("brand"),
        (
            F.lit(100.0)
            + (F.col("id") % 200000).cast("double") / F.lit(100.0)
        ).cast(DecimalType(12, 2)).alias("price"),
        (F.floor(F.rand(204) * 500) + 1)
            .cast("int").alias("stock_quantity"),
        random_choice(205, ["ACTIVE", "ACTIVE", "ACTIVE", "INACTIVE"])
            .alias("product_status"),
        random_timestamp("2023-01-01", "2025-01-01", 206)
            .cast("date").alias("created_date")
    )
)

write_csv(products, "products")

# ============================================================
# 6. ORDERS — 1,000,000 ROWS
# ============================================================

orders_base = (
    spark.range(1, N_ORDERS + 1)
    .withColumn("customer_num", F.floor(F.rand(301) * N_CUSTOMERS) + 1)
    .withColumn("product_num", F.floor(F.rand(302) * N_PRODUCTS) + 1)
    .withColumn("quantity", (F.floor(F.rand(303) * 5) + 1).cast("int"))
    .withColumn("unit_price_value",
                F.lit(100.0) + F.col("product_num") % 200000 / 100.0)
    .withColumn("order_status",
                random_choice(304, ["CREATED", "CONFIRMED", "SHIPPED",
                                    "DELIVERED", "CANCELLED"],
                              [0.10, 0.15, 0.15, 0.50, 0.10]))
    .withColumn("payment_status",
                random_choice(305, ["PAID", "PENDING", "FAILED", "REFUNDED"],
                              [0.78, 0.08, 0.08, 0.06]))
    .withColumn("order_timestamp",
                random_timestamp("2024-01-01", "2026-01-01", 306))
)

orders = orders_base.select(
    F.format_string("O%08d", F.col("id")).alias("order_id"),
    F.format_string("C%05d", F.col("customer_num")).alias("customer_id"),
    F.format_string("P%06d", F.col("product_num")).alias("product_id"),
    F.col("quantity"),
    F.col("unit_price_value").cast(DecimalType(12, 2)).alias("unit_price"),
    (F.col("unit_price_value") * F.col("quantity"))
        .cast(DecimalType(14, 2)).alias("order_amount"),
    "order_status",
    "payment_status",
    "order_timestamp"
)

write_csv(orders, "orders")

# ============================================================
# 7. PAYMENTS — 1,000,000 ROWS
# ============================================================

payments = (
    spark.range(1, N_PAYMENTS + 1)
    .withColumn("customer_num", F.floor(F.rand(401) * N_CUSTOMERS) + 1)
    .withColumn("payment_method",
                random_choice(402, ["UPI", "CREDIT_CARD", "DEBIT_CARD",
                                    "NET_BANKING", "WALLET"],
                              [0.40, 0.25, 0.15, 0.12, 0.08]))
    .withColumn("payment_status",
                random_choice(403, ["PAID", "PENDING", "FAILED", "REFUNDED"],
                              [0.78, 0.08, 0.08, 0.06]))
    .withColumn("payment_amount",
                F.least(
                    F.lit(150000.0),
                    F.greatest(
                        F.lit(30.0),
                        F.exp(F.lit(7.0) + F.randn(404))
                    )
                ).cast(DecimalType(12, 2)))
    .withColumn("payment_timestamp",
                random_timestamp("2024-01-01", "2026-01-01", 405))
    .select(
        F.format_string("PAY%09d", F.col("id")).alias("payment_id"),
        F.format_string("O%08d", F.col("id")).alias("order_id"),
        F.format_string("C%05d", F.col("customer_num")).alias("customer_id"),
        "payment_method",
        "payment_status",
        "payment_amount",
        F.format_string("TXN%012d", F.col("id"))
            .alias("transaction_reference"),
        F.date_format("payment_timestamp", "yyyy-MM-dd HH:mm:ss")
            .alias("payment_timestamp")
    )
)

write_csv(payments, "payments")

# ============================================================
# 8. EVENTS — 5,000,000 ROWS
# ============================================================

events_base = (
    spark.range(1, N_EVENTS + 1)
    .withColumn("event_type",
                random_choice(501,
                    ["PRODUCT_VIEWED", "ADD_TO_CART", "ORDER_CREATED",
                     "PAYMENT_COMPLETED", "PAYMENT_FAILED",
                     "ORDER_SHIPPED", "ORDER_DELIVERED"],
                    [0.35, 0.20, 0.15, 0.12, 0.05, 0.08, 0.05]))
    .withColumn("customer_num", F.floor(F.rand(502) * N_CUSTOMERS) + 1)
    .withColumn("order_num", F.floor(F.rand(503) * N_ORDERS) + 1)
    .withColumn("product_num", F.floor(F.rand(504) * N_PRODUCTS) + 1)
    .withColumn("device", random_choice(505, ["mobile", "desktop", "tablet"]))
    .withColumn("quantity", (F.floor(F.rand(506) * 5) + 1).cast("int"))
    .withColumn("cart_value", F.round(F.lit(100.0) + F.rand(507) * 24900.0, 2))
    .withColumn("source",
                random_choice(508, ["WEB", "MOBILE_APP", "API", "STORE"],
                              [0.45, 0.35, 0.15, 0.05]))
    .withColumn("event_timestamp",
                random_timestamp("2025-01-01", "2026-01-01", 509))
)

order_event_types = [
    "ORDER_CREATED", "PAYMENT_COMPLETED", "PAYMENT_FAILED",
    "ORDER_SHIPPED", "ORDER_DELIVERED"
]

product_event_types = ["PRODUCT_VIEWED", "ADD_TO_CART"]

events = events_base.select(
    F.format_string("E%09d", F.col("id")).alias("event_id"),
    "event_type",
    F.format_string("C%05d", F.col("customer_num")).alias("customer_id"),
    F.when(
        F.col("event_type").isin(order_event_types),
        F.format_string("O%08d", F.col("order_num"))
    ).otherwise(F.lit(None).cast("string")).alias("order_id"),
    F.when(
        F.col("event_type").isin(product_event_types),
        F.format_string("P%06d", F.col("product_num"))
    ).otherwise(F.lit(None).cast("string")).alias("product_id"),
    "event_timestamp",
    "source",
    F.to_json(F.struct(
        F.format_string("S%08d", F.col("id")).alias("session_id"),
        "device",
        "quantity",
        "cart_value"
    )).alias("payload")
)

write_csv(events, "events")

# ============================================================
# 9. FINISH
# ============================================================

print("All five source datasets have been written to S3.")
print("Customers:", f"{OUTPUT_PREFIX}/customers.csv")
print("Products:", f"{OUTPUT_PREFIX}/products.csv")
print("Orders:", f"{OUTPUT_PREFIX}/orders.csv")
print("Payments:", f"{OUTPUT_PREFIX}/payments.csv")
print("Events:", f"{OUTPUT_PREFIX}/events.csv")

job.commit()
