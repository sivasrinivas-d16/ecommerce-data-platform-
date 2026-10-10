
from datetime import datetime, timezone

from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import (
    DateType,
    TimestampType,
)

def validate(spark: SparkSession, bucket: str) -> dict:
    """Strict validation of the Raw Events dataset in AWS Glue."""

    dataset = "events"
    events_path = f"s3://{bucket}/raw/events/"

    products_path = f"s3://{bucket}/raw/products/"
    orders_path = f"s3://{bucket}/raw/orders/"
    customers_path = f"s3://{bucket}/raw/customers/"

    checks = []

    def add_check(name, invalid_records, details=""):
        invalid_records = int(invalid_records)
        checks.append({
            "validation_name": name,
            "invalid_records": invalid_records,
            "status": "PASS" if invalid_records == 0 else "FAIL",
            "details": details,
        })

    def count_invalid(df, condition):
        return df.filter(condition).count()

    # --------------------------------------------------
    # 1. Load Raw Events
    # --------------------------------------------------
    events_df = spark.read.parquet(events_path)
    row_count = events_df.count()

    print("Events loaded from Raw layer")
    print("Event count:", row_count)
    events_df.printSchema()

    # These columns are required for the rules below.
    required_columns = [
        "event_id",
        "event_type",
        "customer_id",
        "event_timestamp",
        "source",
        "product_id",
        "order_id",
        "payload",
    ]

    missing_columns = [
        name for name in required_columns
        if name not in events_df.columns
    ]

    add_check(
        "Required Schema",
        len(missing_columns),
        f"Missing columns: {missing_columns}"
        if missing_columns else "All required columns exist",
    )

    if missing_columns:
        return {
            "dataset": dataset,
            "row_count": row_count,
            "overall_status": "FAIL",
            "run_timestamp": datetime.now(timezone.utc).isoformat(),
            "checks": checks,
        }

    # --------------------------------------------------
    # 2. Required values: null, empty, whitespace
    # --------------------------------------------------
    for name in [
        "event_id",
        "event_type",
        "customer_id",
        "event_timestamp",
        "source",
    ]:
        value = F.col(name)

        condition = (
            value.isNull()
            | (F.length(F.trim(value.cast("string"))) == 0)
        )

        add_check(
            f"Required Field: {name}",
            count_invalid(events_df, condition),
        )

    # --------------------------------------------------
    # 3. Event ID format
    # Expected: E followed by exactly 9 digits
    # --------------------------------------------------
    invalid_event_ids = count_invalid(
        events_df,
        F.col("event_id").isNull()
        | ~F.col("event_id").rlike(r"^E[0-9]{9}$"),
    )

    add_check("Event ID Format", invalid_event_ids)

    # --------------------------------------------------
    # 4. Event ID uniqueness
    # Count extra duplicate rows, not just duplicate groups.
    # --------------------------------------------------
    duplicate_event_ids = (
        events_df
        .filter(F.col("event_id").isNotNull())
        .groupBy("event_id")
        .count()
        .filter(F.col("count") > 1)
    )

    duplicate_extra_rows = (
        duplicate_event_ids
        .agg(
            F.coalesce(
                F.sum(F.col("count") - 1), F.lit(0)
            ).alias("invalid")
        )
        .first()["invalid"]
    )

    add_check("Event ID Uniqueness", duplicate_extra_rows)

    # --------------------------------------------------
    # 5. Allowed event types
    # --------------------------------------------------
    valid_event_types = [
        "PRODUCT_VIEWED",
        "ADD_TO_CART",
        "ORDER_CREATED",
        "PAYMENT_COMPLETED",
        "ORDER_SHIPPED",
        "ORDER_DELIVERED",
        "PAYMENT_FAILED",
    ]

    invalid_event_types = count_invalid(
        events_df,
        F.col("event_type").isNull()
        | ~F.col("event_type").isin(valid_event_types),
    )

    add_check("Event Type Validity", invalid_event_types)

    # --------------------------------------------------
    # 6. Customer ID format
    # Expected: C followed by exactly 5 digits
    # --------------------------------------------------
    invalid_customer_ids = count_invalid(
        events_df,
        F.col("customer_id").isNull()
        | ~F.col("customer_id").rlike(r"^C[0-9]{5}$"),
    )

    add_check("Customer ID Format", invalid_customer_ids)

    # --------------------------------------------------
    # 7. Event timestamp: valid and not in the future
    # --------------------------------------------------
    timestamp_type = events_df.schema["event_timestamp"].dataType

    if isinstance(timestamp_type, (DateType, TimestampType)):
        parsed_timestamp = F.col("event_timestamp").cast("timestamp")
    else:
        parsed_timestamp = F.coalesce(
            F.to_timestamp(F.col("event_timestamp")),
            F.to_timestamp(
                F.col("event_timestamp"),
                "yyyy-MM-dd'T'HH:mm:ss.SSSXXX",
            ),
            F.to_timestamp(
                F.col("event_timestamp"),
                "yyyy-MM-dd HH:mm:ss",
            ),
        )

    invalid_timestamps = count_invalid(
        events_df,
        F.col("event_timestamp").isNull()
        | parsed_timestamp.isNull()
        | (parsed_timestamp > F.current_timestamp()),
    )

    add_check("Event Timestamp Validity", invalid_timestamps)

    # --------------------------------------------------
    # 8. Allowed event sources
    # --------------------------------------------------
    valid_sources = ["WEB", "MOBILE_APP", "API", "STORE"]

    invalid_sources = count_invalid(
        events_df,
        F.col("source").isNull()
        | ~F.col("source").isin(valid_sources),
    )

    add_check("Event Source Validity", invalid_sources)

    # Source distribution is informational, not a pass/fail rule.
    print("Event source distribution:")
    events_df.groupBy("source").count().orderBy(
        F.col("count").desc()
    ).show(truncate=False)

    # --------------------------------------------------
    # 9. Event-specific reference requirements
    # --------------------------------------------------
    product_events = ["PRODUCT_VIEWED", "ADD_TO_CART"]

    order_events = [
        "ORDER_CREATED",
        "PAYMENT_COMPLETED",
        "ORDER_SHIPPED",
        "ORDER_DELIVERED",
        "PAYMENT_FAILED",
    ]

    invalid_product_event_refs = count_invalid(
        events_df,
        F.col("event_type").isin(product_events)
        & (
            F.col("product_id").isNull()
            | (F.length(F.trim(F.col("product_id"))) == 0)
        ),
    )

    add_check(
        "Product Event Reference Required",
        invalid_product_event_refs,
    )

    invalid_order_event_refs = count_invalid(
        events_df,
        F.col("event_type").isin(order_events)
        & (
            F.col("order_id").isNull()
            | (F.length(F.trim(F.col("order_id"))) == 0)
        ),
    )

    add_check(
        "Order Event Reference Required",
        invalid_order_event_refs,
    )

    # --------------------------------------------------
    # 10. Load Raw reference datasets
    # --------------------------------------------------
    products_df = spark.read.parquet(products_path)
    orders_df = spark.read.parquet(orders_path)
    customers_df = spark.read.parquet(customers_path)

    # Check reference schemas before performing joins.
    reference_schemas = {
        "products": (products_df, "product_id"),
        "orders": (orders_df, "order_id"),
        "customers": (customers_df, "customer_id"),
    }

    missing_reference_columns = []

    for reference_name, (reference_df, key) in reference_schemas.items():
        if key not in reference_df.columns:
            missing_reference_columns.append(
                f"{reference_name}.{key}"
            )

    add_check(
        "Reference Dataset Schemas",
        len(missing_reference_columns),
        f"Missing keys: {missing_reference_columns}"
        if missing_reference_columns else "Reference keys exist",
    )

    if missing_reference_columns:
        return {
            "dataset": dataset,
            "row_count": row_count,
            "overall_status": "FAIL",
            "run_timestamp": datetime.now(timezone.utc).isoformat(),
            "checks": checks,
        }

    # --------------------------------------------------
    # 11. Product referential integrity
    # Only product-related event types require a product.
    # --------------------------------------------------
    event_products = (
        events_df
        .filter(F.col("event_type").isin(product_events))
        .select("product_id")
        .filter(F.col("product_id").isNotNull())
        .distinct()
    )

    known_products = (
        products_df
        .select("product_id")
        .filter(F.col("product_id").isNotNull())
        .distinct()
    )

    invalid_product_references = (
        event_products
        .join(known_products, on="product_id", how="left_anti")
        .count()
    )

    add_check(
        "Product Referential Integrity",
        invalid_product_references,
    )

    # --------------------------------------------------
    # 12. Order referential integrity
    # --------------------------------------------------
    event_orders = (
        events_df
        .filter(F.col("event_type").isin(order_events))
        .select("order_id")
        .filter(F.col("order_id").isNotNull())
        .distinct()
    )

    known_orders = (
        orders_df
        .select("order_id")
        .filter(F.col("order_id").isNotNull())
        .distinct()
    )

    invalid_order_references = (
        event_orders
        .join(known_orders, on="order_id", how="left_anti")
        .count()
    )

    add_check(
        "Order Referential Integrity",
        invalid_order_references,
    )

    # --------------------------------------------------
    # 13. Customer referential integrity
    # --------------------------------------------------
    event_customers = (
        events_df
        .select("customer_id")
        .filter(F.col("customer_id").isNotNull())
        .distinct()
    )

    known_customers = (
        customers_df
        .select("customer_id")
        .filter(F.col("customer_id").isNotNull())
        .distinct()
    )

    invalid_customer_references = (
        event_customers
        .join(known_customers, on="customer_id", how="left_anti")
        .count()
    )

    add_check(
        "Customer Referential Integrity",
        invalid_customer_references,
    )

    # --------------------------------------------------
    # 14. Payload session ID
    # Preserves your existing rule that payload itself
    # matches S followed by exactly 8 digits.
    # --------------------------------------------------
    invalid_payload_ids = count_invalid(
        events_df,
        F.col("payload").isNull()
        | ~F.col("payload").rlike(r"^S[0-9]{8}$"),
    )

    add_check("Payload Session ID Format", invalid_payload_ids)

    # --------------------------------------------------
    # 15. Final report
    # --------------------------------------------------
    overall_status = (
        "PASS"
        if all(check["status"] == "PASS" for check in checks)
        else "FAIL"
    )

    report = {
        "dataset": dataset,
        "row_count": row_count,
        "overall_status": overall_status,
        "run_timestamp": datetime.now(timezone.utc).isoformat(),
        "checks": checks,
    }

    print("Events Validation Summary:")

    for check in checks:
        print(
            f"{check['validation_name']}: "
            f"{check['status']} "
            f"(invalid records: {check['invalid_records']})"
        )

    print("Overall Event Validation Status:", overall_status)

    return report
