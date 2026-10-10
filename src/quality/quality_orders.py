
from datetime import datetime, timezone

from pyspark.sql import functions as F


# ============================================================
# ORDERS DATA QUALITY CONFIGURATION
# ============================================================

DATASET_NAME = "orders"

QUALITY_COLUMNS = [
    "order_id",
    "customer_id",
    "product_id",
    "quantity",
    "unit_price",
    "order_amount",
    "order_status",
    "payment_status",
    "order_timestamp",
]

ORDER_ID_PATTERN = r"^O[0-9]{8}$"
CUSTOMER_ID_PATTERN = r"^C[0-9]{5}$"
PRODUCT_ID_PATTERN = r"^P[0-9]{6}$"

VALID_ORDER_STATUSES = [
    "CREATED",
    "CONFIRMED",
    "SHIPPED",
    "DELIVERED",
    "CANCELLED",
]

VALID_PAYMENT_STATUSES = [
    "PAID",
    "PENDING",
    "FAILED",
    "REFUNDED",
]


# ============================================================
# HELPER FUNCTIONS
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


# ============================================================
# MAIN QUALITY FUNCTION
# ============================================================

def run_quality(
    spark,
    orders_path,
    customers_path,
    products_path,
    quality_report_path=None,
):
    """
    Assess order quality using the Raw or Curated orders dataset.

    Returns a dictionary compatible with quality_main.py.
    """

    run_timestamp = datetime.now(timezone.utc).isoformat()

    print("=" * 70)
    print("ORDERS DATA QUALITY")
    print("=" * 70)
    print("Orders path:", orders_path)

    orders_df = spark.read.parquet(orders_path)

    customers_df = spark.read.parquet(
        customers_path
    ).select("customer_id").dropDuplicates()

    products_df = spark.read.parquet(
        products_path
    ).select("product_id").dropDuplicates()

    # Normalize whitespace in string columns.
    for field in orders_df.schema.fields:
        if field.dataType.simpleString() == "string":
            orders_df = orders_df.withColumn(
                field.name,
                F.trim(F.col(field.name)),
            )

    missing_columns = sorted(
        set(QUALITY_COLUMNS) - set(orders_df.columns)
    )

    if missing_columns:
        raise ValueError(
            "Orders dataset is missing required columns: "
            + ", ".join(missing_columns)
        )

    total_records = orders_df.count()

    print("Total order records:", total_records)
    orders_df.printSchema()

    if total_records == 0:
        raise ValueError(
            "Orders dataset is empty; quality score cannot be calculated."
        )

    metrics = []

    # ========================================================
    # 1. COMPLETENESS
    # ========================================================

    print("\n1. ORDER COMPLETENESS")

    completeness_expressions = [
        F.sum(
            F.when(
                F.col(column_name).isNotNull()
                & (F.trim(F.col(column_name)) != ""),
                1,
            ).otherwise(0)
        ).alias(column_name)
        for column_name in QUALITY_COLUMNS
    ]

    completeness_row = orders_df.agg(
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

    completeness_score = percentage(
        sum(completeness_scores),
        len(completeness_scores),
    )

    # ========================================================
    # 2. ORDER ID UNIQUENESS
    # ========================================================

    print("\n2. ORDER ID UNIQUENESS")

    duplicate_order_id_groups = (
        orders_df
        .filter(
            F.col("order_id").isNotNull()
            & (F.col("order_id") != "")
        )
        .groupBy("order_id")
        .count()
        .filter(F.col("count") > 1)
    )

    duplicate_group_count = duplicate_order_id_groups.count()

    duplicate_rows = (
        duplicate_order_id_groups
        .agg(F.sum("count").alias("duplicate_rows"))
        .first()["duplicate_rows"]
        or 0
    )

    duplicate_excess = max(
        int(duplicate_rows) - duplicate_group_count,
        0,
    )

    unique_order_records = max(
        total_records - duplicate_excess,
        0,
    )

    uniqueness_score = percentage(
        unique_order_records,
        total_records,
    )

    print("Duplicate order ID groups:", duplicate_group_count)
    print("Order ID uniqueness:", f"{uniqueness_score:.2f}%")

    add_metric(
        metrics,
        "Order ID Uniqueness",
        unique_order_records,
        total_records,
        "Duplicate order IDs reduce the uniqueness score.",
    )

    # ========================================================
    # 3. ORDER ID FORMAT
    # ========================================================

    valid_order_id_count = orders_df.filter(
        F.col("order_id").rlike(ORDER_ID_PATTERN)
    ).count()

    order_id_format_score = percentage(
        valid_order_id_count,
        total_records,
    )

    add_metric(
        metrics,
        "Order ID Format",
        valid_order_id_count,
        total_records,
        "Order IDs must match O followed by eight digits.",
    )

    # ========================================================
    # 4. CUSTOMER ID AND PRODUCT ID FORMAT
    # ========================================================

    valid_customer_id_count = orders_df.filter(
        F.col("customer_id").rlike(CUSTOMER_ID_PATTERN)
    ).count()

    customer_id_score = percentage(
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

    valid_product_id_count = orders_df.filter(
        F.col("product_id").rlike(PRODUCT_ID_PATTERN)
    ).count()

    product_id_score = percentage(
        valid_product_id_count,
        total_records,
    )

    add_metric(
        metrics,
        "Product ID Format",
        valid_product_id_count,
        total_records,
        "Product IDs must match P followed by six digits.",
    )

    # ========================================================
    # 5. QUANTITY VALIDITY
    # ========================================================

    valid_quantity_count = orders_df.filter(
        F.col("quantity").isNotNull()
        & (F.col("quantity") > 0)
    ).count()

    quantity_score = percentage(
        valid_quantity_count,
        total_records,
    )

    add_metric(
        metrics,
        "Quantity Validity",
        valid_quantity_count,
        total_records,
        "Quantity must be a positive number.",
    )

    # ========================================================
    # 6. UNIT PRICE VALIDITY
    # ========================================================

    valid_unit_price_count = orders_df.filter(
        F.col("unit_price").isNotNull()
        & (F.col("unit_price") >= 0)
    ).count()

    unit_price_score = percentage(
        valid_unit_price_count,
        total_records,
    )

    add_metric(
        metrics,
        "Unit Price Validity",
        valid_unit_price_count,
        total_records,
        "Unit price must be non-negative.",
    )

    # ========================================================
    # 7. ORDER AMOUNT VALIDITY
    # ========================================================

    valid_order_amount_count = orders_df.filter(
        F.col("order_amount").isNotNull()
        & (F.col("order_amount") >= 0)
    ).count()

    order_amount_score = percentage(
        valid_order_amount_count,
        total_records,
    )

    add_metric(
        metrics,
        "Order Amount Validity",
        valid_order_amount_count,
        total_records,
        "Order amount must be non-negative.",
    )

    # ========================================================
    # 8. ORDER AMOUNT CONSISTENCY
    # ========================================================

    expected_amount = F.round(
        F.col("quantity") * F.col("unit_price"),
        2,
    ).cast("decimal(14,2)")

    consistent_amount_count = orders_df.filter(
        F.col("quantity").isNotNull()
        & F.col("unit_price").isNotNull()
        & F.col("order_amount").isNotNull()
        & (
            F.round(F.col("order_amount"), 2)
            == expected_amount
        )
    ).count()

    amount_consistency_score = percentage(
        consistent_amount_count,
        total_records,
    )

    add_metric(
        metrics,
        "Order Amount Consistency",
        consistent_amount_count,
        total_records,
        "Order amount must equal quantity multiplied by unit price, rounded to two decimals.",
    )

    # ========================================================
    # 9. ORDER STATUS VALIDITY
    # ========================================================

    valid_order_status_count = orders_df.filter(
        F.col("order_status").isin(VALID_ORDER_STATUSES)
    ).count()

    order_status_score = percentage(
        valid_order_status_count,
        total_records,
    )

    add_metric(
        metrics,
        "Order Status Validity",
        valid_order_status_count,
        total_records,
        "Order status must be in the configured allowed list.",
    )

    # ========================================================
    # 10. PAYMENT STATUS VALIDITY
    # ========================================================

    valid_payment_status_count = orders_df.filter(
        F.col("payment_status").isin(VALID_PAYMENT_STATUSES)
    ).count()

    payment_status_score = percentage(
        valid_payment_status_count,
        total_records,
    )

    add_metric(
        metrics,
        "Payment Status Validity",
        valid_payment_status_count,
        total_records,
        "Payment status must be in the configured allowed list.",
    )

    # ========================================================
    # 11. ORDER TIMESTAMP VALIDITY
    # ========================================================

    valid_order_timestamp_count = orders_df.filter(
        F.col("order_timestamp").isNotNull()
        & (
            F.col("order_timestamp")
            <= F.current_timestamp()
        )
    ).count()

    order_timestamp_score = percentage(
        valid_order_timestamp_count,
        total_records,
    )

    add_metric(
        metrics,
        "Order Timestamp Validity",
        valid_order_timestamp_count,
        total_records,
        "Order timestamp must be populated and not in the future.",
    )

    # ========================================================
    # 12. CUSTOMER REFERENTIAL INTEGRITY
    # ========================================================

    customer_reference_rows = orders_df.filter(
        F.col("customer_id").isNotNull()
        & (F.col("customer_id") != "")
    )

    valid_customer_references = (
        customer_reference_rows
        .join(
            customers_df,
            on="customer_id",
            how="left_semi",
        )
        .count()
    )

    total_customer_references = customer_reference_rows.count()

    customer_reference_score = percentage(
        valid_customer_references,
        total_customer_references,
    )

    add_metric(
        metrics,
        "Customer Referential Integrity",
        valid_customer_references,
        total_customer_references,
        "Order customer IDs must exist in Raw customers.",
    )

    # ========================================================
    # 13. PRODUCT REFERENTIAL INTEGRITY
    # ========================================================

    product_reference_rows = orders_df.filter(
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

    product_reference_score = percentage(
        valid_product_references,
        total_product_references,
    )

    add_metric(
        metrics,
        "Product Referential Integrity",
        valid_product_references,
        total_product_references,
        "Order product IDs must exist in Raw products.",
    )

    # ========================================================
    # 14. OVERALL QUALITY SCORE
    # ========================================================

    overall_quality_score = round(
        sum(metric["metric_score"] for metric in metrics)
        / len(metrics),
        2,
    )

    overall_status = (
        "PASS"
        if overall_quality_score >= 95.0
        and order_id_format_score == 100.0
        and uniqueness_score == 100.0
        and order_status_score == 100.0
        and payment_status_score == 100.0
        else "FAIL"
    )

    print("\n" + "=" * 70)
    print("ORDERS QUALITY SUMMARY")
    print("=" * 70)

    for metric in metrics:
        print(
            f"{metric['metric_name']:<38}"
            f"{metric['metric_score']:>8.2f}%"
        )

    print("Overall quality score:", f"{overall_quality_score:.2f}%")
    print("Overall status:", overall_status)

    # ========================================================
    # 15. QUALITY REPORT
    # ========================================================

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
