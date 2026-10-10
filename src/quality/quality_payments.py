
from datetime import datetime, timezone

from pyspark.sql import functions as F


# ============================================================
# PAYMENTS DATA QUALITY CONFIGURATION
# ============================================================

DATASET_NAME = "payments"

QUALITY_COLUMNS = [
    "payment_id",
    "order_id",
    "customer_id",
    "payment_method",
    "payment_status",
    "payment_amount",
    "transaction_reference",
    "payment_timestamp",
]

PAYMENT_ID_PATTERN = r"^PAY[0-9]{9}$"
ORDER_ID_PATTERN = r"^O[0-9]{8}$"
CUSTOMER_ID_PATTERN = r"^C[0-9]{5}$"
TRANSACTION_REFERENCE_PATTERN = r"^TXN[0-9]{12}$"

VALID_PAYMENT_STATUSES = [
    "PAID",
    "PENDING",
    "FAILED",
    "REFUNDED",
]

VALID_PAYMENT_METHODS = [
    "UPI",
    "CREDIT_CARD",
    "DEBIT_CARD",
    "NET_BANKING",
    "WALLET",
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


def duplicate_statistics(df, column_name):
    """
    Return duplicate group count and duplicate excess row count.
    Null and blank values are excluded from the uniqueness test.
    """

    duplicate_groups = (
        df.filter(
            F.col(column_name).isNotNull()
            & (F.trim(F.col(column_name)) != "")
        )
        .groupBy(column_name)
        .count()
        .filter(F.col("count") > 1)
    )

    group_count = duplicate_groups.count()

    duplicate_rows = (
        duplicate_groups
        .agg(F.sum("count").alias("duplicate_rows"))
        .first()["duplicate_rows"]
        or 0
    )

    duplicate_excess = max(
        int(duplicate_rows) - group_count,
        0,
    )

    return group_count, duplicate_excess


# ============================================================
# MAIN QUALITY FUNCTION
# ============================================================

def run_quality(
    spark,
    payments_path,
    orders_path,
    customers_path,
    quality_report_path=None,
):
    """
    Run payment quality checks.

    Returns a dictionary compatible with quality_main.py.
    """

    run_timestamp = datetime.now(timezone.utc).isoformat()

    print("=" * 70)
    print("PAYMENTS DATA QUALITY")
    print("=" * 70)
    print("Payments path:", payments_path)

    payments_df = spark.read.parquet(payments_path)

    orders_df = (
        spark.read.parquet(orders_path)
        .select("order_id")
        .dropDuplicates()
    )

    customers_df = (
        spark.read.parquet(customers_path)
        .select("customer_id")
        .dropDuplicates()
    )

    # Trim whitespace in string columns.
    for field in payments_df.schema.fields:
        if field.dataType.simpleString() == "string":
            payments_df = payments_df.withColumn(
                field.name,
                F.trim(F.col(field.name)),
            )

    missing_columns = sorted(
        set(QUALITY_COLUMNS) - set(payments_df.columns)
    )

    if missing_columns:
        raise ValueError(
            "Payments dataset is missing required columns: "
            + ", ".join(missing_columns)
        )

    total_records = payments_df.count()

    print("Total payment records:", total_records)
    payments_df.printSchema()

    if total_records == 0:
        raise ValueError(
            "Payments dataset is empty; quality score cannot be calculated."
        )

    metrics = []

    # ========================================================
    # 1. COMPLETENESS
    # ========================================================

    print("\n1. PAYMENT COMPLETENESS")

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

    completeness_row = payments_df.agg(
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
    # 2. PAYMENT ID UNIQUENESS
    # ========================================================

    print("\n2. PAYMENT ID UNIQUENESS")

    duplicate_payment_groups, duplicate_payment_excess = (
        duplicate_statistics(payments_df, "payment_id")
    )

    unique_payment_records = max(
        total_records - duplicate_payment_excess,
        0,
    )

    payment_id_uniqueness = percentage(
        unique_payment_records,
        total_records,
    )

    print("Duplicate payment ID groups:", duplicate_payment_groups)
    print("Payment ID uniqueness:", f"{payment_id_uniqueness:.2f}%")

    add_metric(
        metrics,
        "Payment ID Uniqueness",
        unique_payment_records,
        total_records,
        "Duplicate payment IDs reduce the uniqueness score.",
    )

    # ========================================================
    # 3. PAYMENT ID FORMAT
    # ========================================================

    valid_payment_id_count = payments_df.filter(
        F.col("payment_id").rlike(PAYMENT_ID_PATTERN)
    ).count()

    payment_id_format_score = percentage(
        valid_payment_id_count,
        total_records,
    )

    add_metric(
        metrics,
        "Payment ID Format",
        valid_payment_id_count,
        total_records,
        "Payment IDs must match PAY followed by nine digits.",
    )

    # ========================================================
    # 4. TRANSACTION REFERENCE UNIQUENESS
    # ========================================================

    duplicate_transaction_groups, duplicate_transaction_excess = (
        duplicate_statistics(
            payments_df,
            "transaction_reference",
        )
    )

    unique_transaction_records = max(
        total_records - duplicate_transaction_excess,
        0,
    )

    transaction_reference_uniqueness = percentage(
        unique_transaction_records,
        total_records,
    )

    print(
        "Duplicate transaction reference groups:",
        duplicate_transaction_groups,
    )
    print(
        "Transaction reference uniqueness:",
        f"{transaction_reference_uniqueness:.2f}%",
    )

    add_metric(
        metrics,
        "Transaction Reference Uniqueness",
        unique_transaction_records,
        total_records,
        "Transaction references should be unique.",
    )

    # ========================================================
    # 5. TRANSACTION REFERENCE FORMAT
    # ========================================================

    valid_transaction_reference_count = payments_df.filter(
        F.col("transaction_reference").rlike(
            TRANSACTION_REFERENCE_PATTERN
        )
    ).count()

    transaction_reference_format_score = percentage(
        valid_transaction_reference_count,
        total_records,
    )

    add_metric(
        metrics,
        "Transaction Reference Format",
        valid_transaction_reference_count,
        total_records,
        "Transaction references must match TXN followed by twelve digits.",
    )

    # ========================================================
    # 6. ORDER ID FORMAT
    # ========================================================

    valid_order_id_count = payments_df.filter(
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
    # 7. CUSTOMER ID FORMAT
    # ========================================================

    valid_customer_id_count = payments_df.filter(
        F.col("customer_id").rlike(CUSTOMER_ID_PATTERN)
    ).count()

    customer_id_format_score = percentage(
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

    # ========================================================
    # 8. PAYMENT AMOUNT VALIDITY
    # ========================================================

    valid_payment_amount_count = payments_df.filter(
        F.col("payment_amount").isNotNull()
        & (F.col("payment_amount") > 0)
    ).count()

    payment_amount_validity = percentage(
        valid_payment_amount_count,
        total_records,
    )

    add_metric(
        metrics,
        "Payment Amount Validity",
        valid_payment_amount_count,
        total_records,
        "Payment amount must be greater than zero.",
    )

    # ========================================================
    # 9. PAYMENT STATUS VALIDITY
    # ========================================================

    valid_payment_status_count = payments_df.filter(
        F.col("payment_status").isin(
            VALID_PAYMENT_STATUSES
        )
    ).count()

    payment_status_validity = percentage(
        valid_payment_status_count,
        total_records,
    )

    add_metric(
        metrics,
        "Payment Status Validity",
        valid_payment_status_count,
        total_records,
        "Payment status must be in the allowed status list.",
    )

    # ========================================================
    # 10. PAYMENT METHOD VALIDITY
    # ========================================================

    valid_payment_method_count = payments_df.filter(
        F.col("payment_method").isin(
            VALID_PAYMENT_METHODS
        )
    ).count()

    payment_method_validity = percentage(
        valid_payment_method_count,
        total_records,
    )

    add_metric(
        metrics,
        "Payment Method Validity",
        valid_payment_method_count,
        total_records,
        "Payment method must be in the allowed method list.",
    )

    # ========================================================
    # 11. PAYMENT TIMESTAMP VALIDITY
    # ========================================================

    valid_payment_timestamp_count = payments_df.filter(
        F.col("payment_timestamp").isNotNull()
        & (
            F.col("payment_timestamp")
            <= F.current_timestamp()
        )
    ).count()

    payment_timestamp_validity = percentage(
        valid_payment_timestamp_count,
        total_records,
    )

    add_metric(
        metrics,
        "Payment Timestamp Validity",
        valid_payment_timestamp_count,
        total_records,
        "Payment timestamp must be populated and not in the future.",
    )

    # ========================================================
    # 12. ORDER REFERENTIAL INTEGRITY
    # ========================================================

    payment_order_references = payments_df.filter(
        F.col("order_id").isNotNull()
        & (F.col("order_id") != "")
    )

    valid_order_references = (
        payment_order_references
        .join(
            orders_df,
            on="order_id",
            how="left_semi",
        )
        .count()
    )

    total_order_references = payment_order_references.count()

    order_referential_integrity = percentage(
        valid_order_references,
        total_order_references,
    )

    add_metric(
        metrics,
        "Order Referential Integrity",
        valid_order_references,
        total_order_references,
        "Payment order IDs must exist in the Raw orders dataset.",
    )

    # ========================================================
    # 13. CUSTOMER REFERENTIAL INTEGRITY
    # ========================================================

    payment_customer_references = payments_df.filter(
        F.col("customer_id").isNotNull()
        & (F.col("customer_id") != "")
    )

    valid_customer_references = (
        payment_customer_references
        .join(
            customers_df,
            on="customer_id",
            how="left_semi",
        )
        .count()
    )

    total_customer_references = payment_customer_references.count()

    customer_referential_integrity = percentage(
        valid_customer_references,
        total_customer_references,
    )

    add_metric(
        metrics,
        "Customer Referential Integrity",
        valid_customer_references,
        total_customer_references,
        "Payment customer IDs must exist in Raw customers.",
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
        and payment_id_uniqueness == 100.0
        and transaction_reference_uniqueness == 100.0
        and payment_id_format_score == 100.0
        and transaction_reference_format_score == 100.0
        and payment_status_validity == 100.0
        and payment_method_validity == 100.0
        else "FAIL"
    )

    print("\n" + "=" * 70)
    print("PAYMENTS QUALITY SUMMARY")
    print("=" * 70)

    for metric in metrics:
        print(
            f"{metric['metric_name']:<40}"
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
