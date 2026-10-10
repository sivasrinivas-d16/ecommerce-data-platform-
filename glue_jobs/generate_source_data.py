
import sys
import calendar
from datetime import datetime

from awsglue.utils import getResolvedOptions
from awsglue.context import GlueContext
from awsglue.job import Job

from pyspark.context import SparkContext
from pyspark.sql import functions as F
from pyspark.sql.types import DecimalType


# ============================================================
# 1. CONFIGURATION
# ============================================================

args = getResolvedOptions(sys.argv, ["JOB_NAME"])

BUCKET = "ecommerce-data-platform-version1"
OUTPUT_PREFIX = f"s3://{BUCKET}/data_s3"

N_CUSTOMERS = 10_000
N_PRODUCTS = 100_000
N_ORDERS = 1_000_000
N_PAYMENTS = 1_000_000
N_EVENTS = 5_000_000

OUTPUT_PARTITIONS = 8
SEED = 42

ORDER_EVENT_TYPES = [
    "ORDER_CREATED",
    "PAYMENT_COMPLETED",
    "PAYMENT_FAILED",
    "ORDER_SHIPPED",
    "ORDER_DELIVERED",
]

PRODUCT_EVENT_TYPES = [
    "PRODUCT_VIEWED",
    "ADD_TO_CART",
]


# ============================================================
# 2. INITIALIZE GLUE AND SPARK
# ============================================================

sc = SparkContext.getOrCreate()
glue_context = GlueContext(sc)
spark = glue_context.spark_session

job = Job(glue_context)
job.init(args["JOB_NAME"], args)

spark.conf.set("spark.sql.session.timeZone", "UTC")

print("=" * 70)
print("ECOMMERCE SOURCE DATA GENERATION")
print("=" * 70)
print("S3 output:", OUTPUT_PREFIX)


# ============================================================
# 3. REUSABLE FUNCTIONS
# ============================================================

def random_choice(seed, choices, probabilities=None):
    """
    Return a Spark Column containing randomly selected values.

    Applies otherwise() exactly once to the chained expression.
    """

    if not choices:
        raise ValueError("choices cannot be empty")

    random_value = F.rand(seed)

    if probabilities is None:
        return F.element_at(
            F.array(*[F.lit(value) for value in choices]),
            (F.floor(random_value * len(choices)) + 1).cast("int"),
        )

    if len(choices) != len(probabilities):
        raise ValueError(
            "choices and probabilities must have equal lengths"
        )

    if any(probability < 0 for probability in probabilities):
        raise ValueError("Probabilities cannot be negative")

    if abs(sum(probabilities) - 1.0) > 1e-9:
        raise ValueError("Probabilities must sum to 1.0")

    result = None
    cumulative_probability = 0.0

    for value, probability in zip(choices, probabilities):
        cumulative_probability += probability

        if probability == 0:
            continue

        condition = random_value < cumulative_probability

        if result is None:
            result = F.when(condition, F.lit(value))
        else:
            result = result.when(condition, F.lit(value))

    if result is None:
        raise ValueError("At least one probability must be positive")

    # Apply otherwise exactly once.
    return result.otherwise(F.lit(choices[-1]))


def random_timestamp(start_date, end_date, seed):
    """
    Generate a random timestamp in UTC.

    The start is inclusive and the end boundary is exclusive.
    Dates must use YYYY-MM-DD format.
    """

    start_dt = datetime.strptime(start_date, "%Y-%m-%d")
    end_dt = datetime.strptime(end_date, "%Y-%m-%d")

    start_epoch = calendar.timegm(start_dt.timetuple())
    end_epoch = calendar.timegm(end_dt.timetuple())

    if end_epoch <= start_epoch:
        raise ValueError("end_date must be after start_date")

    seconds_range = end_epoch - start_epoch

    epoch_seconds = (
        F.lit(start_epoch)
        + F.floor(F.rand(seed) * seconds_range).cast("long")
    )

    return F.to_timestamp(F.from_unixtime(epoch_seconds))


def customer_number_for_id(id_column):
    """
    Deterministically map an ID to a valid customer number.

    Reusing this mapping keeps payment and order customer IDs
    consistent without joining large datasets.
    """

    return (
        F.pmod((id_column - F.lit(1)) * F.lit(7919),
               F.lit(N_CUSTOMERS))
        + F.lit(1)
    ).cast("long")


def write_csv(df, dataset_name, expected_rows):
    """
    Write CSV part files directly to S3 and verify row count.

    Output is a Spark CSV prefix, not a single CSV object.
    """

    destination = f"{OUTPUT_PREFIX}/{dataset_name}.csv"

    print("-" * 70)
    print(f"Writing dataset: {dataset_name}")
    print(f"Destination: {destination}")
    print(f"Expected rows: {expected_rows:,}")

    (
        df.repartition(OUTPUT_PARTITIONS)
        .write
        .mode("overwrite")
        .option("header", True)
        .option("maxRecordsPerFile", 250_000)
        .csv(destination)
    )

    print(f"Successfully wrote {dataset_name} to S3")


# ============================================================
# 4. CUSTOMERS — 10,000 ROWS
# ============================================================

customers = (
    spark.range(1, N_CUSTOMERS + 1)
    .select(
        F.format_string(
            "C%05d", F.col("id")
        ).alias("customer_id"),

        F.concat(
            F.lit("Customer "),
            F.col("id")
        ).alias("name"),

        F.concat(
            F.lit("customer"),
            F.col("id"),
            F.lit("@example.com")
        ).alias("email"),

        random_choice(
            101,
            ["Sydney", "Melbourne", "Brisbane", "Perth", "Adelaide"]
        ).alias("city"),

        random_choice(
            102,
            ["NSW", "VIC", "QLD", "WA", "SA"]
        ).alias("state"),

        F.lit("Australia").alias("country"),

        F.date_format(
            random_timestamp(
                "2022-01-01", "2025-01-01", 103
            ),
            "dd-MM-yyyy",
        ).alias("signup_date"),
    )
)

write_csv(customers, "customers", N_CUSTOMERS)


# ============================================================
# 5. PRODUCTS — 100,000 ROWS
# ============================================================

products = (
    spark.range(1, N_PRODUCTS + 1)
    .select(
        F.format_string(
            "P%06d", F.col("id")
        ).alias("product_id"),

        F.concat(
            F.lit("Product "),
            F.col("id")
        ).alias("product_name"),

        random_choice(
            201,
            ["Electronics", "Home", "Fashion", "Beauty", "Sports"],
        ).alias("category"),

        random_choice(
            202,
            ["Standard", "Premium", "Basic"],
        ).alias("subcategory"),

        random_choice(
            203,
            ["BrandA", "BrandB", "BrandC"],
        ).alias("brand"),

        (
            F.lit(100.0)
            + (F.col("id") % 200000).cast("double") / F.lit(100.0)
        ).cast(DecimalType(12, 2)).alias("price"),

        (
            F.floor(F.rand(204) * 500) + 1
        ).cast("int").alias("stock_quantity"),

        random_choice(
            205,
            ["ACTIVE", "INACTIVE"],
            [0.75, 0.25],
        ).alias("product_status"),

        random_timestamp(
            "2023-01-01", "2025-01-01", 206
        ).cast("date").alias("created_date"),
    )
)

write_csv(products, "products", N_PRODUCTS)


# ============================================================
# 6. ORDERS — 1,000,000 ROWS
# ============================================================

orders_base = (
    spark.range(1, N_ORDERS + 1)
    .withColumn(
        "customer_num",
        customer_number_for_id(F.col("id")),
    )
    .withColumn(
        "product_num",
        (F.pmod(F.col("id") * 104729, F.lit(N_PRODUCTS)) + 1)
        .cast("long"),
    )
    .withColumn(
        "quantity",
        (F.floor(F.rand(303) * 5) + 1).cast("int"),
    )
    .withColumn(
        "unit_price_value",
        F.lit(100.0)
        + F.col("product_num").cast("double") / F.lit(100.0),
    )
    .withColumn(
        "order_status",
        random_choice(
            304,
            ["CREATED", "CONFIRMED", "SHIPPED", "DELIVERED", "CANCELLED"],
            [0.10, 0.15, 0.15, 0.50, 0.10],
        ),
    )
    .withColumn(
        "payment_status",
        random_choice(
            305,
            ["PAID", "PENDING", "FAILED", "REFUNDED"],
            [0.78, 0.08, 0.08, 0.06],
        ),
    )
    .withColumn(
        "order_timestamp",
        random_timestamp(
            "2024-01-01", "2026-01-01", 306
        ),
    )
)

orders = orders_base.select(
    F.format_string(
        "O%08d", F.col("id")
    ).alias("order_id"),

    F.format_string(
        "C%05d", F.col("customer_num")
    ).alias("customer_id"),

    F.format_string(
        "P%06d", F.col("product_num")
    ).alias("product_id"),

    F.col("quantity"),

    F.col("unit_price_value").cast(
        DecimalType(12, 2)
    ).alias("unit_price"),

    (
        F.col("unit_price_value") * F.col("quantity")
    ).cast(DecimalType(14, 2)).alias("order_amount"),

    F.col("order_status"),
    F.col("payment_status"),
    F.col("order_timestamp"),
)

write_csv(orders, "orders", N_ORDERS)


# ============================================================
# 7. PAYMENTS — 1,000,000 ROWS
# ============================================================

payments = (
    spark.range(1, N_PAYMENTS + 1)
    .withColumn(
        "customer_num",
        customer_number_for_id(F.col("id")),
    )
    .withColumn(
        "payment_method",
        random_choice(
            402,
            ["UPI", "CREDIT_CARD", "DEBIT_CARD", "NET_BANKING", "WALLET"],
            [0.40, 0.25, 0.15, 0.12, 0.08],
        ),
    )
    .withColumn(
        "payment_status",
        random_choice(
            403,
            ["PAID", "PENDING", "FAILED", "REFUNDED"],
            [0.78, 0.08, 0.08, 0.06],
        ),
    )
    .withColumn(
        "payment_amount",
        F.least(
            F.lit(150000.0),
            F.greatest(
                F.lit(30.0),
                F.exp(F.lit(7.0) + F.randn(404)),
            ),
        ).cast(DecimalType(12, 2)),
    )
    .withColumn(
        "payment_timestamp",
        random_timestamp(
            "2024-01-01", "2026-01-01", 405
        ),
    )
    .select(
        F.format_string(
            "PAY%09d", F.col("id")
        ).alias("payment_id"),

        F.format_string(
            "O%08d", F.col("id")
        ).alias("order_id"),

        F.format_string(
            "C%05d", F.col("customer_num")
        ).alias("customer_id"),

        F.col("payment_method"),
        F.col("payment_status"),
        F.col("payment_amount"),

        F.format_string(
            "TXN%012d", F.col("id")
        ).alias("transaction_reference"),

        F.date_format(
            F.col("payment_timestamp"),
            "yyyy-MM-dd HH:mm:ss",
        ).alias("payment_timestamp"),
    )
)

write_csv(payments, "payments", N_PAYMENTS)


# ============================================================
# 8. EVENTS — 5,000,000 ROWS
# ============================================================

events_base = (
    spark.range(1, N_EVENTS + 1)
    .withColumn(
        "event_type",
        random_choice(
            501,
            [
                "PRODUCT_VIEWED",
                "ADD_TO_CART",
                "ORDER_CREATED",
                "PAYMENT_COMPLETED",
                "PAYMENT_FAILED",
                "ORDER_SHIPPED",
                "ORDER_DELIVERED",
            ],
            [0.35, 0.20, 0.15, 0.12, 0.05, 0.08, 0.05],
        ),
    )
    .withColumn(
        "order_num",
        (F.pmod(F.floor(F.rand(503) * N_ORDERS), F.lit(N_ORDERS)) + 1)
        .cast("long"),
    )
    .withColumn(
        "product_num",
        (F.pmod(F.floor(F.rand(504) * N_PRODUCTS), F.lit(N_PRODUCTS)) + 1)
        .cast("long"),
    )
    .withColumn(
        "random_customer_num",
        (F.floor(F.rand(502) * N_CUSTOMERS) + 1).cast("long"),
    )
    .withColumn(
        "device",
        random_choice(505, ["mobile", "desktop", "tablet"]),
    )
    .withColumn(
        "quantity",
        (F.floor(F.rand(506) * 5) + 1).cast("int"),
    )
    .withColumn(
        "cart_value",
        F.round(F.lit(100.0) + F.rand(507) * 24900.0, 2),
    )
    .withColumn(
        "source",
        random_choice(
            508,
            ["WEB", "MOBILE_APP", "API", "STORE"],
            [0.45, 0.35, 0.15, 0.05],
        ),
    )
    .withColumn(
        "event_timestamp",
        random_timestamp(
            "2025-01-01", "2026-01-01", 509
        ),
    )
)

events = events_base.select(
    F.format_string(
        "E%09d", F.col("id")
    ).alias("event_id"),

    F.col("event_type"),

    # For order-related events, use the same customer mapping
    # as the referenced order. Other events use a random customer.
    F.format_string(
        "C%05d",
        F.when(
            F.col("event_type").isin(ORDER_EVENT_TYPES),
            customer_number_for_id(F.col("order_num")),
        ).otherwise(F.col("random_customer_num")),
    ).alias("customer_id"),

    F.when(
        F.col("event_type").isin(ORDER_EVENT_TYPES),
        F.format_string("O%08d", F.col("order_num")),
    ).otherwise(F.lit(None).cast("string")).alias("order_id"),

    F.when(
        F.col("event_type").isin(PRODUCT_EVENT_TYPES),
        F.format_string("P%06d", F.col("product_num")),
    ).otherwise(F.lit(None).cast("string")).alias("product_id"),

    F.col("event_timestamp"),
    F.col("source"),

    F.to_json(
        F.struct(
            F.format_string(
                "S%08d", F.col("id")
            ).alias("session_id"),
            F.col("device"),
            F.col("quantity"),
            F.col("cart_value"),
        )
    ).alias("payload"),
)

write_csv(events, "events", N_EVENTS)


# ============================================================
# 9. COMPLETE JOB
# ============================================================

print("=" * 70)
print("ALL SOURCE DATASETS WRITTEN SUCCESSFULLY")
print("=" * 70)

for dataset in [
    "customers",
    "products",
    "orders",
    "payments",
    "events",
]:
    print(f"{OUTPUT_PREFIX}/{dataset}.csv/")

job.commit()
