
"""Build the curated e-commerce analytical model from refined Parquet data.

Dimensions:
    dim_customers, dim_products, dim_date

Facts:
    fact_orders, fact_payments, fact_events

Integrated reporting:
    integrated_orders, integrated_customer_activity

Aggregates:
    agg_sales_daily, agg_product_performance,
    agg_customer_performance, agg_payment_performance,
    agg_event_activity_daily

Payments remain independent because the current payment schema has no
verified order/customer foreign key. Events are aggregated before joining
to orders to avoid multiplying order revenue.
"""
from __future__ import annotations

import logging
import sys
from typing import Iterable

import boto3
from awsglue.context import GlueContext
from awsglue.job import Job
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from pyspark.sql import DataFrame, Window
from pyspark.sql import functions as F
from pyspark.sql.types import (
    BooleanType,
    DateType,
    DecimalType,
    DoubleType,
    IntegerType,
    LongType,
    StringType,
    TimestampType,
)

REGION = "ap-southeast-2"
BUCKET = "ecommerce-data-platform-version1"
DATABASE = "ecommerce_data_platform"
BASE = f"s3://{BUCKET}/"

LOG = logging.getLogger("ecommerce-curated-layer")
LOG.setLevel(logging.INFO)

args = getResolvedOptions(sys.argv, ["JOB_NAME"])
sc = SparkContext.getOrCreate()
glue_context = GlueContext(sc)
spark = glue_context.spark_session

job = Job(glue_context)
job.init(args["JOB_NAME"], args)


# ------------------------------------------------------------
# Common helpers
# ------------------------------------------------------------

def read_refined(name: str) -> DataFrame:
    path = f"{BASE}refined/{name}/"
    LOG.info("Reading %s from %s", name, path)
    return spark.read.parquet(path)


def require_columns(
    df: DataFrame,
    dataset: str,
    columns: Iterable[str],
) -> None:
    missing = sorted(set(columns) - set(df.columns))
    if missing:
        raise ValueError(
            f"Refined {dataset} is missing required columns: {missing}"
        )


def select_existing(
    df: DataFrame,
    names: Iterable[str],
) -> DataFrame:
    return df.select(*[name for name in names if name in df.columns])


def assert_unique_nonnull(
    df: DataFrame,
    key: str,
    table: str,
) -> None:
    invalid = df.filter(
        F.col(key).isNull()
        | (F.trim(F.col(key).cast("string")) == "")
    )

    if invalid.limit(1).count():
        raise ValueError(
            f"{table}.{key} contains null or blank keys"
        )

    duplicates = (
        df.groupBy(key)
        .count()
        .filter(F.col("count") > 1)
    )

    if duplicates.limit(1).count():
        examples = [
            row[key]
            for row in duplicates.select(key).limit(5).collect()
        ]
        raise ValueError(
            f"{table}.{key} is not unique. Examples: {examples}"
        )


def glue_type(data_type) -> str:
    if isinstance(data_type, StringType):
        return "string"
    if isinstance(data_type, BooleanType):
        return "boolean"
    if isinstance(data_type, IntegerType):
        return "int"
    if isinstance(data_type, LongType):
        return "bigint"
    if isinstance(data_type, DoubleType):
        return "double"
    if isinstance(data_type, DecimalType):
        return (
            f"decimal({data_type.precision},"
            f"{data_type.scale})"
        )
    if isinstance(data_type, DateType):
        return "date"
    if isinstance(data_type, TimestampType):
        return "timestamp"
    return "string"


def publish_table(
    df: DataFrame,
    table: str,
    relative_path: str,
    key: str | None = None,
) -> int:
    """Write Parquet to S3 and create/update its Glue Catalog table."""
    row_count = df.count()

    if key and row_count:
        assert_unique_nonnull(df, key, table)

    path = f"{BASE}curated/{relative_path}/"

    (
        df.write
        .mode("overwrite")
        .format("parquet")
        .option("compression", "snappy")
        .save(path)
    )

    glue = boto3.client("glue", region_name=REGION)

    columns = [
        {"Name": field.name, "Type": glue_type(field.dataType)}
        for field in df.schema.fields
    ]

    table_input = {
        "Name": table,
        "TableType": "EXTERNAL_TABLE",
        "Parameters": {
            "classification": "parquet",
            "EXTERNAL": "TRUE",
        },
        "StorageDescriptor": {
            "Columns": columns,
            "Location": path,
            "InputFormat": (
                "org.apache.hadoop.hive.ql.io.parquet."
                "MapredParquetInputFormat"
            ),
            "OutputFormat": (
                "org.apache.hadoop.hive.ql.io.parquet."
                "MapredParquetOutputFormat"
            ),
            "Compressed": True,
            "SerdeInfo": {
                "SerializationLibrary": (
                    "org.apache.hadoop.hive.ql.io.parquet."
                    "serde.ParquetHiveSerDe"
                ),
                "Parameters": {"serialization.format": "1"},
            },
            "StoredAsSubDirectories": False,
        },
    }

    try:
        glue.get_table(
            DatabaseName=DATABASE,
            Name=table,
        )
        glue.update_table(
            DatabaseName=DATABASE,
            TableInput=table_input,
        )
    except glue.exceptions.EntityNotFoundException:
        glue.create_table(
            DatabaseName=DATABASE,
            TableInput=table_input,
        )

    persisted_count = spark.read.parquet(path).count()

    if persisted_count != row_count:
        raise ValueError(
            f"Post-write count mismatch for {table}: "
            f"{row_count} vs {persisted_count}"
        )

    LOG.info(
        "Published table=%s rows=%s path=%s",
        table,
        row_count,
        path,
    )
    return row_count


# ------------------------------------------------------------
# 1. Read refined data and validate required schemas
# ------------------------------------------------------------

customers = read_refined("customers")
products = read_refined("products")
orders = read_refined("orders")
payments = read_refined("payments")
events = read_refined("events")

require_columns(
    customers,
    "customers",
    [
        "customer_id", "name", "email", "signup_date",
        "city", "state", "country",
        "customer_data_quality_status",
    ],
)

require_columns(
    products,
    "products",
    [
        "product_id", "product_name", "category",
        "subcategory", "brand", "price",
        "stock_quantity", "product_status",
        "product_data_quality_status",
    ],
)

require_columns(
    orders,
    "orders",
    [
        "order_id", "customer_id", "product_id",
        "order_timestamp", "order_date", "quantity",
        "unit_price", "order_amount", "order_status",
        "payment_status", "order_data_quality_status",
        "order_amount_valid",
    ],
)

require_columns(
    payments,
    "payments",
    [
        "payment_id", "payment_method", "payment_status",
        "transaction_reference", "payment_timestamp",
        "payment_amount", "payment_data_quality_status",
    ],
)

require_columns(
    events,
    "events",
    [
        "event_id", "event_type", "source",
        "customer_id", "order_id", "product_id",
        "event_timestamp", "event_category",
        "event_data_quality_status",
    ],
)


# ------------------------------------------------------------
# 2. Dimensions
# ------------------------------------------------------------

dim_customers = select_existing(
    customers,
    [
        "customer_id", "name", "email", "signup_date",
        "signup_year", "signup_month", "signup_day",
        "city", "state", "country",
        "customer_tenure_days", "customer_status",
        "customer_data_quality_status",
    ],
).filter(
    F.col("customer_id").isNotNull()
    & (F.trim(F.col("customer_id")) != "")
)

dim_products = select_existing(
    products,
    [
        "product_id", "product_name", "category",
        "subcategory", "brand", "price",
        "price_category", "stock_quantity",
        "stock_status", "product_status",
        "product_data_quality_status",
    ],
).filter(
    F.col("product_id").isNotNull()
    & (F.trim(F.col("product_id")) != "")
)

assert_unique_nonnull(
    dim_customers, "customer_id", "dim_customers"
)
assert_unique_nonnull(
    dim_products, "product_id", "dim_products"
)

# Stop if fact IDs are duplicated. Do not arbitrarily choose a record.
for source_df, key, table in [
    (orders, "order_id", "refined_orders"),
    (payments, "payment_id", "refined_payments"),
    (events, "event_id", "refined_events"),
]:
    valid_keys = source_df.filter(
        F.col(key).isNotNull()
        & (F.trim(F.col(key)) != "")
    )
    assert_unique_nonnull(valid_keys, key, table)


# ------------------------------------------------------------
# 3. Date dimension
# ------------------------------------------------------------

date_sources = []

for source_df, column_name in [
    (customers, "signup_date"),
    (orders, "order_date"),
    (payments, "payment_date"),
    (events, "event_date"),
]:
    if column_name in source_df.columns:
        date_sources.append(
            source_df.select(
                F.col(column_name)
                .cast("date")
                .alias("calendar_date")
            )
        )

if not date_sources:
    raise ValueError("No date columns available for dim_date")

all_dates = date_sources[0]

for source_df in date_sources[1:]:
    all_dates = all_dates.unionByName(source_df)

all_dates = all_dates.filter(
    F.col("calendar_date").isNotNull()
)

date_span = all_dates.agg(
    F.min("calendar_date").alias("min_date"),
    F.max("calendar_date").alias("max_date"),
).first()

if date_span["min_date"] is None:
    raise ValueError("No valid dates found for dim_date")

dim_date = spark.sql(
    "SELECT explode(sequence("
    f"to_date('{date_span['min_date']}'), "
    f"to_date('{date_span['max_date']}'), "
    "interval 1 day)) AS calendar_date"
).select(
    "calendar_date",
    F.year("calendar_date").alias("year"),
    F.quarter("calendar_date").alias("quarter"),
    F.month("calendar_date").alias("month"),
    F.date_format(
        "calendar_date", "MMMM"
    ).alias("month_name"),
    F.dayofmonth(
        "calendar_date"
    ).alias("day_of_month"),
    F.dayofweek(
        "calendar_date"
    ).alias("day_of_week_sunday_1"),
    F.date_format(
        "calendar_date", "EEEE"
    ).alias("day_name"),
    F.weekofyear(
        "calendar_date"
    ).alias("week_of_year"),
    F.dayofweek("calendar_date")
    .isin(1, 7)
    .alias("is_weekend"),
)


# ------------------------------------------------------------
# 4. Detailed facts: preserve source grain and quality flags
# ------------------------------------------------------------

fact_orders = select_existing(
    orders,
    [
        "order_id", "customer_id", "product_id",
        "order_timestamp", "order_date",
        "order_year", "order_month", "order_day",
        "quantity", "unit_price", "order_amount",
        "calculated_order_amount", "order_amount_valid",
        "order_status", "payment_status",
        "is_completed_order", "is_paid_order",
        "net_order_amount", "order_data_quality_status",
    ],
)

fact_payments = select_existing(
    payments,
    [
        "payment_id", "payment_method", "payment_status",
        "transaction_reference", "payment_timestamp",
        "payment_date", "payment_year", "payment_month",
        "payment_day", "payment_amount",
        "payment_value_category", "payment_result",
        "payment_data_quality_status",
    ],
)

fact_events = select_existing(
    events,
    [
        "event_id", "event_type", "event_category", "source",
        "payload", "customer_id", "order_id", "product_id",
        "event_timestamp", "event_date", "event_year",
        "event_month", "event_day", "event_hour",
        "event_data_quality_status",
    ],
)


# # ------------------------------------------------------------
# # 5. Order KPI eligibility
# # ------------------------------------------------------------

# # REVIEW and invalid records remain in facts. They are excluded from
# # eligible revenue and reported separately in quality-related metrics.
# orders_kpi = orders.withColumn(
#     "is_eligible_completed_order",
#     (F.upper(F.trim(F.col("order_status"))) == "COMPLETED")
#     & (
#         F.upper(
#             F.trim(F.col("order_data_quality_status"))
#         ) == "VALID"
#     )
#     & (F.col("order_amount_valid") == F.lit(True))
#     & F.col("order_id").isNotNull()
#     & (F.trim(F.col("order_id")) != ""),
# )

# orders_kpi = (
#     orders_kpi
#     .withColumn(
#         "eligible_revenue",
#         F.when(
#             F.col("is_eligible_completed_order"),
#             F.col("order_amount"),
#         ).otherwise(F.lit(0).cast("decimal(18,2)")),
#     )
#     .withColumn(
#         "eligible_units",
#         F.when(
#             F.col("is_eligible_completed_order"),
#             F.col("quantity"),
#         ).otherwise(F.lit(0).cast("long")),
#     )
# )

# ------------------------------------------------------------
# 5. Sales eligibility
# ------------------------------------------------------------
# Keep all source records in the fact tables.
# Only delivered, paid, quality-valid orders with valid amounts
# and valid order IDs contribute to sales KPIs.

orders_kpi = orders.withColumn(
    "is_sales_eligible_order",
    (F.upper(F.trim(F.col("order_status"))) == "DELIVERED")
    & (F.upper(F.trim(F.col("payment_status"))) == "PAID")
    & (
        F.upper(F.trim(F.col("order_data_quality_status")))
        == "VALID"
    )
    & (F.col("order_amount_valid") == F.lit(True))
    & F.col("order_id").isNotNull()
    & (F.trim(F.col("order_id")) != ""),
)

orders_kpi = (
    orders_kpi
    .withColumn(
        "sales_eligible_revenue",
        F.when(
            F.col("is_sales_eligible_order"),
            F.col("order_amount"),
        ).otherwise(F.lit(0).cast("decimal(18,2)")),
    )
    .withColumn(
        "sales_eligible_units",
        F.when(
            F.col("is_sales_eligible_order"),
            F.col("quantity"),
        ).otherwise(F.lit(0).cast("long")),
    )
)


# ------------------------------------------------------------
# 6. Integrated order table
# ------------------------------------------------------------

customer_lookup = dim_customers.select(
    F.col("customer_id").alias("_dim_customer_id"),
    F.col("name").alias("customer_name"),
    F.col("email").alias("customer_email"),
    F.col("signup_date").alias("customer_signup_date"),
    F.col("city").alias("customer_city"),
    F.col("state").alias("customer_state"),
    F.col("country").alias("customer_country"),
    F.col("customer_status"),
    F.col("customer_data_quality_status"),
)

product_lookup = dim_products.select(
    F.col("product_id").alias("_dim_product_id"),
    F.col("product_name"),
    F.col("category").alias("product_category"),
    F.col("subcategory").alias("product_subcategory"),
    F.col("brand").alias("product_brand"),
    F.col("price").alias("current_product_price"),
    F.col("stock_quantity").alias("current_stock_quantity"),
    F.col("stock_status").alias("current_stock_status"),
    F.col("product_data_quality_status"),
)

integrated_orders = (
    orders_kpi
    .join(
        customer_lookup,
        orders_kpi.customer_id
        == customer_lookup._dim_customer_id,
        "left",
    )
    .join(
        product_lookup,
        orders_kpi.product_id
        == product_lookup._dim_product_id,
        "left",
    )
    .withColumn(
        "customer_match_status",
        F.when(
            F.col("_dim_customer_id").isNull(), "UNMATCHED"
        ).otherwise("MATCHED"),
    )
    .withColumn(
        "product_match_status",
        F.when(
            F.col("_dim_product_id").isNull(), "UNMATCHED"
        ).otherwise("MATCHED"),
    )
    .drop("_dim_customer_id", "_dim_product_id")
)

# Aggregate events before joining. This prevents one order being repeated
# for each related event.
event_by_order = (
    fact_events
    .filter(
        F.col("order_id").isNotNull()
        & (F.trim(F.col("order_id")) != "")
    )
    .groupBy("order_id")
    .agg(
        F.countDistinct("event_id").alias(
            "related_event_count"
        ),
        F.countDistinct(
            F.when(
                F.col("event_category") == "FULFILLMENT",
                F.col("event_id"),
            )
        ).alias("fulfillment_event_count"),
        F.max(
            F.when(
                F.col("event_category") == "FULFILLMENT",
                F.col("event_timestamp"),
            )
        ).alias("last_fulfillment_event_timestamp"),
    )
)

integrated_orders = (
    integrated_orders
    .join(event_by_order, "order_id", "left")
    .fillna(
        {
            "related_event_count": 0,
            "fulfillment_event_count": 0,
        }
    )
)


# ------------------------------------------------------------
# 7. Dashboard aggregates
# ------------------------------------------------------------


# 7.1 Daily sales: delivered and paid orders passing validation

agg_sales_daily = (
    orders_kpi
    .groupBy("order_date")
    .agg(
        F.countDistinct(
            F.when(
                F.col("is_sales_eligible_order"),
                F.col("order_id"),
            )
        ).alias("sales_eligible_orders"),

        F.sum("sales_eligible_revenue").alias(
            "sales_eligible_revenue"
        ),

        F.sum("sales_eligible_units").alias(
            "sales_eligible_units_sold"
        ),

        F.countDistinct("order_id").alias(
            "all_distinct_orders"
        ),

        F.countDistinct(
            F.when(
                (F.upper(F.trim(F.col("order_status"))) == "DELIVERED")
                & (
                    F.upper(F.trim(F.col("payment_status")))
                    == "PAID"
                ),
                F.col("order_id"),
            )
        ).alias("delivered_paid_orders_before_quality_filter"),

        F.countDistinct(
            F.when(
                F.upper(F.trim(F.col("order_data_quality_status")))
                != "VALID",
                F.col("order_id"),
            )
        ).alias("orders_not_valid_quality"),

        F.countDistinct(
            F.when(
                F.col("order_amount_valid") == F.lit(False),
                F.col("order_id"),
            )
        ).alias("order_amount_mismatches"),
    )
    .withColumn(
        "average_order_value",
        F.when(
            F.col("sales_eligible_orders") > 0,
            F.col("sales_eligible_revenue")
            / F.col("sales_eligible_orders"),
        ),
    )
)


# 7.2 Product performance
product_sales = (
    orders_kpi
    .groupBy("product_id")
    .agg(
        F.sum("eligible_revenue").alias("eligible_revenue"),
        F.sum("eligible_units").alias("eligible_units_sold"),
        F.countDistinct(
            F.when(
                F.col("is_eligible_completed_order"),
                F.col("order_id"),
            )
        ).alias("eligible_completed_orders"),
    )
)

agg_product_performance = (
    dim_products
    .join(product_sales, "product_id", "left")
    .fillna(
        {
            "eligible_revenue": 0,
            "eligible_units_sold": 0,
            "eligible_completed_orders": 0,
        }
    )
    .withColumn(
        "revenue_rank",
        F.dense_rank().over(
            Window.orderBy(
                F.col("eligible_revenue").desc_nulls_last()
            )
        ),
    )
)

# 7.3 Customer purchase and activity metrics
order_by_customer = (
    orders_kpi
    .filter(
        F.col("customer_id").isNotNull()
        & (F.trim(F.col("customer_id")) != "")
    )
    .groupBy("customer_id")
    .agg(
        F.countDistinct(
            F.when(
                F.col("is_eligible_completed_order"),
                F.col("order_id"),
            )
        ).alias("eligible_completed_orders"),
        F.sum("eligible_revenue").alias(
            "eligible_customer_revenue"
        ),
        F.sum("eligible_units").alias("eligible_units_sold"),
        F.min(
            F.when(
                F.col("is_eligible_completed_order"),
                F.col("order_date"),
            )
        ).alias("first_eligible_order_date"),
        F.max(
            F.when(
                F.col("is_eligible_completed_order"),
                F.col("order_date"),
            )
        ).alias("last_eligible_order_date"),
    )
)

event_by_customer = (
    fact_events
    .filter(
        F.col("customer_id").isNotNull()
        & (F.trim(F.col("customer_id")) != "")
    )
    .groupBy("customer_id")
    .agg(
        F.countDistinct("event_id").alias("event_count"),
        F.countDistinct(
            F.when(
                F.col("event_category") == "BROWSING",
                F.col("event_id"),
            )
        ).alias("browsing_event_count"),
        F.countDistinct(
            F.when(
                F.col("event_category") == "ORDER",
                F.col("event_id"),
            )
        ).alias("order_event_count"),
        F.countDistinct(
            F.when(
                F.col("event_category") == "PAYMENT",
                F.col("event_id"),
            )
        ).alias("payment_event_count"),
    )
)

agg_customer_performance = (
    dim_customers
    .join(order_by_customer, "customer_id", "left")
    .join(event_by_customer, "customer_id", "left")
    .fillna(
        {
            "eligible_completed_orders": 0,
            "eligible_customer_revenue": 0,
            "eligible_units_sold": 0,
            "event_count": 0,
            "browsing_event_count": 0,
            "order_event_count": 0,
            "payment_event_count": 0,
        }
    )
    .withColumn(
        "is_repeat_customer",
        F.col("eligible_completed_orders") >= 2,
    )
    .withColumn(
        "customer_behavior_segment",
        F.when(
            F.col("eligible_completed_orders") == 0,
            "NO_ELIGIBLE_PURCHASE",
        )
        .when(
            F.col("eligible_completed_orders") == 1,
            "ONE_ELIGIBLE_PURCHASE",
        )
        .otherwise("REPEAT_CUSTOMER"),
    )
)

# 7.4 Payments remain independent of orders until a valid relationship
# is established in the source schema.
agg_payment_performance = (
    fact_payments
    .groupBy("payment_date", "payment_method")
    .agg(
        F.countDistinct("payment_id").alias("payment_attempts"),
        F.countDistinct(
            F.when(
                F.col("payment_result") == "SUCCESS",
                F.col("payment_id"),
            )
        ).alias("successful_payments"),
        F.countDistinct(
            F.when(
                F.col("payment_result") == "FAILED",
                F.col("payment_id"),
            )
        ).alias("failed_payments"),
        F.countDistinct(
            F.when(
                F.col("payment_result") == "PENDING",
                F.col("payment_id"),
            )
        ).alias("pending_payments"),
        F.countDistinct(
            F.when(
                F.col("payment_result") == "REFUNDED",
                F.col("payment_id"),
            )
        ).alias("refunded_payments"),
        F.countDistinct(
            F.when(
                F.col("payment_result").isin(
                    "SUCCESS", "FAILED", "REFUNDED"
                ),
                F.col("payment_id"),
            )
        ).alias("known_final_outcome_attempts"),
        F.sum(
            F.when(
                (F.col("payment_result") == "SUCCESS")
                & (
                    F.upper(F.col("payment_data_quality_status"))
                    == "VALID"
                ),
                F.col("payment_amount"),
            ).otherwise(F.lit(0).cast("decimal(18,2)"))
        ).alias("valid_success_payment_amount"),
        F.countDistinct(
            F.when(
                F.upper(F.col("payment_data_quality_status"))
                != "VALID",
                F.col("payment_id"),
            )
        ).alias("payment_records_needing_review"),
    )
    .withColumn(
        "payment_success_rate",
        F.when(
            F.col("known_final_outcome_attempts") > 0,
            F.col("successful_payments")
            / F.col("known_final_outcome_attempts"),
        ),
    )
)

# 7.5 Event activity
agg_event_activity_daily = (
    fact_events
    .groupBy(
        "event_date",
        "event_type",
        "event_category",
        "source",
    )
    .agg(
        F.countDistinct("event_id").alias("event_count"),
        F.countDistinct("customer_id").alias(
            "identified_customers"
        ),
        F.countDistinct("order_id").alias("identified_orders"),
        F.countDistinct("product_id").alias("identified_products"),
        F.countDistinct(
            F.when(
                F.upper(F.col("event_data_quality_status"))
                != "VALID",
                F.col("event_id"),
            )
        ).alias("events_needing_review"),
    )
)


# ------------------------------------------------------------
# 8. Publish curated outputs to S3 and Glue Catalog
# ------------------------------------------------------------

outputs = [
    (
        dim_customers,
        "dim_customers",
        "dimensions/dim_customers",
        "customer_id",
    ),
    (
        dim_products,
        "dim_products",
        "dimensions/dim_products",
        "product_id",
    ),
    (
        dim_date,
        "dim_date",
        "dimensions/dim_date",
        "calendar_date",
    ),
    (
        fact_orders,
        "fact_orders",
        "facts/fact_orders",
        None,
    ),
    (
        fact_payments,
        "fact_payments",
        "facts/fact_payments",
        None,
    ),
    (
        fact_events,
        "fact_events",
        "facts/fact_events",
        None,
    ),
    (
        integrated_orders,
        "integrated_orders",
        "integrated/integrated_orders",
        None,
    ),
    (
        agg_customer_performance,
        "integrated_customer_activity",
        "integrated/integrated_customer_activity",
        "customer_id",
    ),
    (
        agg_sales_daily,
        "agg_sales_daily",
        "aggregates/agg_sales_daily",
        None,
    ),
    (
        agg_product_performance,
        "agg_product_performance",
        "aggregates/agg_product_performance",
        "product_id",
    ),
    (
        agg_customer_performance,
        "agg_customer_performance",
        "aggregates/agg_customer_performance",
        "customer_id",
    ),
    (
        agg_payment_performance,
        "agg_payment_performance",
        "aggregates/agg_payment_performance",
        None,
    ),
    (
        agg_event_activity_daily,
        "agg_event_activity_daily",
        "aggregates/agg_event_activity_daily",
        None,
    ),
]

summary = {}

for dataframe, table_name, path, key in outputs:
    summary[table_name] = publish_table(
        dataframe,
        table_name,
        path,
        key,
    )

LOG.info(
    "CURATED_MODEL_SUCCESS table_row_counts=%s",
    summary,
)

job.commit()
