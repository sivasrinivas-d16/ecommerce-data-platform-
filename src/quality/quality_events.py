
from datetime import datetime, timezone

from pyspark.sql import functions as F


# ============================================================
# EVENTS DATA QUALITY
# ============================================================

DATASET_NAME = "events"

QUALITY_COLUMNS = [
    "event_id",
    "event_type",
    "customer_id",
    "event_timestamp",
    "source",
    "payload",
]

VALID_EVENT_TYPES = [
    "PRODUCT_VIEWED",
    "ADD_TO_CART",
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

ORDER_EVENT_TYPES = [
    "ORDER_CREATED",
    "PAYMENT_COMPLETED",
    "PAYMENT_FAILED",
    "ORDER_SHIPPED",
    "ORDER_DELIVERED",
]

VALID_SOURCES = [
    "WEB",
    "MOBILE_APP",
    "API",
    "STORE",
]

EVENT_ID_PATTERN = r"^E[0-9]{9}$"
CUSTOMER_ID_PATTERN = r"^C[0-9]{5}$"
PRODUCT_ID_PATTERN = r"^P[0-9]{6}$"
ORDER_ID_PATTERN = r"^O[0-9]{8}$"
SESSION_ID_PATTERN = r"^S[0-9]{8}$"


# ============================================================
# HELPERS
# ============================================================

def percentage(numerator, denominator):
    if denominator == 0:
        return 0.0

    return round((numerator / denominator) * 100, 2)


def add_metric(
    metrics,
    metric_name,
    passed_count,
    total_count,
    description,
):
    metrics.append({
        "dataset": DATASET_NAME,
        "metric_name": metric_name,
        "metric_score": percentage(
            passed_count,
            total_count,
        ),
        "passed_records": int(passed_count),
        "total_records": int(total_count),
        "failed_records": int(total_count - passed_count),
        "description": description,
    })


def count_rows(df):
    return df.count()


# ============================================================
# MAIN QUALITY FUNCTION
# ============================================================

def run_quality(
    spark,
    events_path,
    customers_path,
    products_path,
    orders_path,
    quality_report_path=None,
):
    """
    Assess event data quality using Raw events, customers,
    products, and orders datasets.

    Returns a dictionary compatible with quality_main.py.
    """

    run_timestamp = datetime.now(timezone.utc).isoformat()

    print("=" * 70)
    print("EVENTS DATA QUALITY")
    print("=" * 70)

    # --------------------------------------------------------
    # 1. LOAD DATA
    # --------------------------------------------------------

    events_df = spark.read.parquet(events_path)

    customers_df = spark.read.parquet(customers_path).select(
        "customer_id"
    ).dropDuplicates()

    products_df = spark.read.parquet(products_path).select(
        "product_id"
    ).dropDuplicates()

    orders_df = spark.read.parquet(orders_path).select(
        "order_id"
    ).dropDuplicates()

    # Trim string columns without modifying timestamp types.
    for field in events_df.schema.fields:
        if field.dataType.simpleString() == "string":
            events_df = events_df.withColumn(
                field.name,
                F.trim(F.col(field.name)),
            )

    total_records = events_df.count()

    print("Events loaded:", total_records)
    events_df.printSchema()

    if total_records == 0:
        raise ValueError(
            "Events dataset is empty; quality score cannot be calculated."
        )

    missing_columns = sorted(
        set(QUALITY_COLUMNS) - set(events_df.columns)
    )

    if missing_columns:
        raise ValueError(
            "Events dataset is missing required columns: "
            + ", ".join(missing_columns)
        )

    # --------------------------------------------------------
    # 2. COMPLETENESS
    # --------------------------------------------------------

    print("\n1. EVENT COMPLETENESS")

    metrics = []

    completeness_expressions = [
        F.sum(
            F.when(
                F.col(column_name).isNotNull()
                & (
                    F.trim(F.col(column_name)) != ""
                    if dict(
                        (field.name, field.dataType.simpleString())
                        for field in events_df.schema.fields
                    )[column_name] == "string"
                    else F.lit(True)
                ),
                1,
            ).otherwise(0)
        ).alias(column_name)
        for column_name in QUALITY_COLUMNS
    ]

    completeness_row = events_df.agg(
        *completeness_expressions
    ).first()

    completeness_scores = []

    for column_name in QUALITY_COLUMNS:
        non_missing = int(
            completeness_row[column_name] or 0
        )

        score = percentage(non_missing, total_records)
        completeness_scores.append(score)

        print(f"{column_name}: {score:.2f}%")

        add_metric(
            metrics,
            f"Completeness - {column_name}",
            non_missing,
            total_records,
            f"Non-null and non-blank {column_name} values.",
        )

    completeness_quality = percentage(
        sum(completeness_scores),
        len(completeness_scores),
    )

    # --------------------------------------------------------
    # 3. EVENT ID UNIQUENESS
    # --------------------------------------------------------

    print("\n2. EVENT ID UNIQUENESS")

    duplicate_event_id_rows = (
        events_df
        .filter(
            F.col("event_id").isNotNull()
            & (F.col("event_id") != "")
        )
        .groupBy("event_id")
        .count()
        .filter(F.col("count") > 1)
        .agg(
            F.sum("count").alias("duplicate_rows")
        )
        .first()["duplicate_rows"]
        or 0
    )

    duplicate_event_id_groups = (
        events_df
        .filter(
            F.col("event_id").isNotNull()
            & (F.col("event_id") != "")
        )
        .groupBy("event_id")
        .count()
        .filter(F.col("count") > 1)
        .count()
    )

    duplicate_excess = max(
        int(duplicate_event_id_rows)
        - int(duplicate_event_id_groups),
        0,
    )

    unique_event_records = max(
        total_records - duplicate_excess,
        0,
    )

    event_id_uniqueness = percentage(
        unique_event_records,
        total_records,
    )

    print("Duplicate event ID groups:", duplicate_event_id_groups)
    print("Event ID uniqueness:", f"{event_id_uniqueness:.2f}%")

    add_metric(
        metrics,
        "Event ID Uniqueness",
        unique_event_records,
        total_records,
        "Duplicate event IDs reduce the uniqueness score.",
    )

    # --------------------------------------------------------
    # 4. EVENT ID FORMAT
    # --------------------------------------------------------

    valid_event_id_count = events_df.filter(
        F.col("event_id").rlike(EVENT_ID_PATTERN)
    ).count()

    event_id_format_score = percentage(
        valid_event_id_count,
        total_records,
    )

    add_metric(
        metrics,
        "Event ID Format",
        valid_event_id_count,
        total_records,
        "Event IDs must match E followed by nine digits.",
    )

    # --------------------------------------------------------
    # 5. EVENT TYPE VALIDITY
    # --------------------------------------------------------

    valid_event_type_count = events_df.filter(
        F.col("event_type").isin(VALID_EVENT_TYPES)
    ).count()

    event_type_validity = percentage(
        valid_event_type_count,
        total_records,
    )

    print("Event type validity:", f"{event_type_validity:.2f}%")

    add_metric(
        metrics,
        "Event Type Validity",
        valid_event_type_count,
        total_records,
        "Event type must be in the configured allowed list.",
    )

    # --------------------------------------------------------
    # 6. CUSTOMER ID FORMAT
    # --------------------------------------------------------

    valid_customer_id_count = events_df.filter(
        F.col("customer_id").rlike(CUSTOMER_ID_PATTERN)
    ).count()

    customer_id_validity = percentage(
        valid_customer_id_count,
        total_records,
    )

    add_metric(
        metrics,
        "Customer ID Format",
        valid_customer_id_count,
        total_records,
        "Customer IDs must match C followed by five digits.",
    )

    # --------------------------------------------------------
    # 7. TIMESTAMP VALIDITY
    # --------------------------------------------------------

    valid_timestamp_count = events_df.filter(
        F.col("event_timestamp").isNotNull()
        & (
            F.col("event_timestamp")
            <= F.current_timestamp()
        )
    ).count()

    event_timestamp_validity = percentage(
        valid_timestamp_count,
        total_records,
    )

    print(
        "Event timestamp validity:",
        f"{event_timestamp_validity:.2f}%",
    )

    add_metric(
        metrics,
        "Event Timestamp Validity",
        valid_timestamp_count,
        total_records,
        "Timestamp must be populated and not in the future.",
    )

    # --------------------------------------------------------
    # 8. SOURCE VALIDITY
    # --------------------------------------------------------

    valid_source_count = events_df.filter(
        F.col("source").isin(VALID_SOURCES)
    ).count()

    event_source_validity = percentage(
        valid_source_count,
        total_records,
    )

    add_metric(
        metrics,
        "Event Source Validity",
        valid_source_count,
        total_records,
        "Source must be WEB, MOBILE_APP, API, or STORE.",
    )

    # --------------------------------------------------------
    # 9. CONDITIONAL PRODUCT ID REQUIREMENT
    # --------------------------------------------------------

    product_event_df = events_df.filter(
        F.col("event_type").isin(PRODUCT_EVENT_TYPES)
    )

    product_event_count = product_event_df.count()

    valid_product_reference_count = product_event_df.filter(
        F.col("product_id").rlike(PRODUCT_ID_PATTERN)
    ).count()

    product_reference_validity = percentage(
        valid_product_reference_count,
        product_event_count,
    )

    print(
        "Product event reference validity:",
        f"{product_reference_validity:.2f}%",
    )

    add_metric(
        metrics,
        "Product Event Reference Validity",
        valid_product_reference_count,
        product_event_count,
        "Product browsing/cart events require a correctly formatted product ID.",
    )

    # --------------------------------------------------------
    # 10. CONDITIONAL ORDER ID REQUIREMENT
    # --------------------------------------------------------

    order_event_df = events_df.filter(
        F.col("event_type").isin(ORDER_EVENT_TYPES)
    )

    order_event_count = order_event_df.count()

    valid_order_reference_count = order_event_df.filter(
        F.col("order_id").rlike(ORDER_ID_PATTERN)
    ).count()

    order_reference_validity = percentage(
        valid_order_reference_count,
        order_event_count,
    )

    print(
        "Order event reference validity:",
        f"{order_reference_validity:.2f}%",
    )

    add_metric(
        metrics,
        "Order Event Reference Validity",
        valid_order_reference_count,
        order_event_count,
        "Order-related events require a correctly formatted order ID.",
    )

    # --------------------------------------------------------
    # 11. PRODUCT REFERENTIAL INTEGRITY
    # --------------------------------------------------------

    product_reference_rows = product_event_df.filter(
        F.col("product_id").isNotNull()
        & (F.col("product_id") != "")
    )

    valid_product_references = (
        product_reference_rows
        .join(
            products_df,
            on="product_id",
            how="left_semi",
        )
        .count()
    )

    total_product_references = product_reference_rows.count()

    product_referential_integrity = percentage(
        valid_product_references,
        total_product_references,
    )

    print(
        "Product referential integrity:",
        f"{product_referential_integrity:.2f}%",
    )

    add_metric(
        metrics,
        "Product Referential Integrity",
        valid_product_references,
        total_product_references,
        "Product references must exist in the Raw products dataset.",
    )

    # --------------------------------------------------------
    # 12. ORDER REFERENTIAL INTEGRITY
    # --------------------------------------------------------

    order_reference_rows = order_event_df.filter(
        F.col("order_id").isNotNull()
        & (F.col("order_id") != "")
    )

    valid_order_references = (
        order_reference_rows
        .join(
            orders_df,
            on="order_id",
            how="left_semi",
        )
        .count()
    )

    total_order_references = order_reference_rows.count()

    order_referential_integrity = percentage(
        valid_order_references,
        total_order_references,
    )

    print(
        "Order referential integrity:",
        f"{order_referential_integrity:.2f}%",
    )

    add_metric(
        metrics,
        "Order Referential Integrity",
        valid_order_references,
        total_order_references,
        "Order references must exist in the Raw orders dataset.",
    )

    # --------------------------------------------------------
    # 13. CUSTOMER REFERENTIAL INTEGRITY
    # --------------------------------------------------------

    valid_customer_references = (
        events_df
        .filter(
            F.col("customer_id").isNotNull()
            & (F.col("customer_id") != "")
        )
        .join(
            customers_df,
            on="customer_id",
            how="left_semi",
        )
        .count()
    )

    total_customer_references = events_df.filter(
        F.col("customer_id").isNotNull()
        & (F.col("customer_id") != "")
    ).count()

    customer_referential_integrity = percentage(
        valid_customer_references,
        total_customer_references,
    )

    print(
        "Customer referential integrity:",
        f"{customer_referential_integrity:.2f}%",
    )

    add_metric(
        metrics,
        "Customer Referential Integrity",
        valid_customer_references,
        total_customer_references,
        "Customer references must exist in Raw customers.",
    )

    # --------------------------------------------------------
    # 14. PAYLOAD JSON AND SESSION ID VALIDITY
    # --------------------------------------------------------

    print("\nPayload JSON and session ID validity")

    payload_schema = """
        session_id STRING,
        device STRING,
        quantity INT,
        cart_value DOUBLE
    """

    parsed_events = events_df.withColumn(
        "_payload",
        F.from_json(
            F.col("payload"),
            payload_schema,
        ),
    )

    valid_payload_count = parsed_events.filter(
        F.col("_payload").isNotNull()
        & F.col("_payload.session_id").rlike(SESSION_ID_PATTERN)
        & F.col("_payload.device").isin(
            "mobile",
            "desktop",
            "tablet",
        )
        & F.col("_payload.quantity").between(1, 5)
        & (F.col("_payload.cart_value") >= 0)
    ).count()

    payload_validity = percentage(
        valid_payload_count,
        total_records,
    )

    print("Valid payloads:", valid_payload_count)
    print("Payload validity:", f"{payload_validity:.2f}%")

    add_metric(
        metrics,
        "Payload Validity",
        valid_payload_count,
        total_records,
        "Payload must be valid JSON with a valid session ID, device, quantity, and cart value.",
    )

    # --------------------------------------------------------
    # 15. OVERALL QUALITY SCORE
    # --------------------------------------------------------

    overall_quality_score = round(
        sum(metric["metric_score"] for metric in metrics)
        / len(metrics),
        2,
    )

    overall_status = (
        "PASS"
        if overall_quality_score >= 95.0
        and event_id_uniqueness == 100.0
        and event_id_format_score == 100.0
        and event_type_validity == 100.0
        and event_timestamp_validity == 100.0
        else "FAIL"
    )

    print("\n" + "=" * 70)
    print("EVENTS QUALITY SUMMARY")
    print("=" * 70)

    for metric in metrics:
        print(
            f"{metric['metric_name']:<38}"
            f"{metric['metric_score']:>8.2f}%"
        )

    print("Overall quality score:", f"{overall_quality_score:.2f}%")
    print("Overall status:", overall_status)

    # --------------------------------------------------------
    # 16. PERSIST REPORT
    # --------------------------------------------------------

    for metric in metrics:
        metric["overall_quality_score"] = overall_quality_score
        metric["overall_status"] = overall_status
        metric["run_timestamp"] = run_timestamp

    report = {
        "dataset": DATASET_NAME,
        "overall_status": overall_status,
        "overall_quality_score": overall_quality_score,
        "total_records": total_records,
        "metrics": metrics,
        "generated_at": run_timestamp,
    }

    if quality_report_path:
        report_df = spark.createDataFrame(metrics)

        report_df.write.mode("overwrite").parquet(
            quality_report_path
        )

        print("Quality report saved:", quality_report_path)

        spark.read.parquet(
            quality_report_path
        ).show(truncate=False)

    return report
