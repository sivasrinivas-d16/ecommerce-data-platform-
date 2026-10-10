
from datetime import datetime, timezone

from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import DateType, TimestampType


def validate(spark: SparkSession, bucket: str) -> dict:
    """Strict validation of Raw Payments using AWS Glue."""

    dataset = "payments"

    payments_path = f"s3://{bucket}/raw/payments/"
    orders_path = f"s3://{bucket}/raw/orders/"
    customers_path = f"s3://{bucket}/raw/customers/"

    checks = []

    def add_check(name, invalid_records, details=""):
        invalid_records = int(invalid_records or 0)
        checks.append({
            "validation_name": name,
            "invalid_records": invalid_records,
            "status": "PASS" if invalid_records == 0 else "FAIL",
            "details": details,
        })

    def count_invalid(df, condition):
        return df.filter(condition).count()

    def blank(column_name):
        value = F.col(column_name)
        return (
            value.isNull()
            | (F.length(F.trim(value.cast("string"))) == 0)
        )

    # --------------------------------------------------
    # 1. Load Raw Payments
    # --------------------------------------------------
    payments_df = spark.read.parquet(payments_path)
    row_count = payments_df.count()

    print("Payments loaded from Raw layer")
    print("Payment count:", row_count)
    payments_df.printSchema()

    required_columns = [
        "payment_id",
        "order_id",
        "customer_id",
        "payment_method",
        "payment_status",
        "payment_amount",
        "transaction_reference",
        "payment_timestamp",
    ]

    missing_columns = [
        name for name in required_columns
        if name not in payments_df.columns
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
    # 2. Required fields
    # Detect null, empty, and whitespace-only values.
    # --------------------------------------------------
    for name in required_columns:
        add_check(
            f"Required Field: {name}",
            count_invalid(payments_df, blank(name)),
        )

    # --------------------------------------------------
    # 3. Payment ID format
    # Expected: PAY followed by exactly 9 digits.
    # --------------------------------------------------
    invalid_payment_ids = count_invalid(
        payments_df,
        F.col("payment_id").isNull()
        | ~F.col("payment_id").rlike(r"^PAY[0-9]{9}$"),
    )

    add_check("Payment ID Format", invalid_payment_ids)

    # --------------------------------------------------
    # 4. Payment ID uniqueness
    # Count duplicate rows beyond the first occurrence.
    # --------------------------------------------------
    duplicate_payment_groups = (
        payments_df
        .filter(F.col("payment_id").isNotNull())
        .groupBy("payment_id")
        .count()
        .filter(F.col("count") > 1)
    )

    duplicate_payment_count = (
        duplicate_payment_groups
        .agg(
            F.coalesce(
                F.sum(F.col("count") - 1), F.lit(0)
            ).alias("invalid")
        )
        .first()["invalid"]
    )

    add_check("Payment ID Uniqueness", duplicate_payment_count)

    # --------------------------------------------------
    # 5. Transaction reference uniqueness
    # --------------------------------------------------
    duplicate_transaction_groups = (
        payments_df
        .filter(F.col("transaction_reference").isNotNull())
        .groupBy("transaction_reference")
        .count()
        .filter(F.col("count") > 1)
    )

    duplicate_transaction_count = (
        duplicate_transaction_groups
        .agg(
            F.coalesce(
                F.sum(F.col("count") - 1), F.lit(0)
            ).alias("invalid")
        )
        .first()["invalid"]
    )

    add_check(
        "Transaction Reference Uniqueness",
        duplicate_transaction_count,
    )

    # --------------------------------------------------
    # 6. Payment amount
    # Amount must be numeric and strictly greater than 0.
    # --------------------------------------------------
    payment_amount = F.col("payment_amount").cast("decimal(20,2)")

    invalid_payment_amounts = count_invalid(
        payments_df,
        F.col("payment_amount").isNotNull()
        & (
            payment_amount.isNull()
            | (payment_amount <= 0)
        ),
    )

    add_check(
        "Payment Amount Must Be Positive",
        invalid_payment_amounts,
    )

    # --------------------------------------------------
    # 7. Payment status
    # --------------------------------------------------
    valid_payment_statuses = [
        "PAID",
        "PENDING",
        "FAILED",
        "REFUNDED",
    ]

    invalid_payment_statuses = count_invalid(
        payments_df,
        F.col("payment_status").isNull()
        | ~F.col("payment_status").isin(valid_payment_statuses),
    )

    add_check("Payment Status Validity", invalid_payment_statuses)

    # --------------------------------------------------
    # 8. Payment method
    # --------------------------------------------------
    valid_payment_methods = [
        "UPI",
        "CREDIT_CARD",
        "DEBIT_CARD",
        "NET_BANKING",
        "WALLET",
    ]

    invalid_payment_methods = count_invalid(
        payments_df,
        F.col("payment_method").isNull()
        | ~F.col("payment_method").isin(valid_payment_methods),
    )

    add_check("Payment Method Validity", invalid_payment_methods)

    # --------------------------------------------------
    # 9. Payment timestamp validity
    # Reject unparseable and future timestamps.
    # --------------------------------------------------
    timestamp_type = payments_df.schema[
        "payment_timestamp"
    ].dataType

    if isinstance(timestamp_type, (DateType, TimestampType)):
        parsed_timestamp = F.col("payment_timestamp").cast("timestamp")
    else:
        parsed_timestamp = F.coalesce(
            F.to_timestamp(F.col("payment_timestamp")),
            F.to_timestamp(
                F.col("payment_timestamp"),
                "yyyy-MM-dd'T'HH:mm:ss.SSSXXX",
            ),
            F.to_timestamp(
                F.col("payment_timestamp"),
                "yyyy-MM-dd HH:mm:ss",
            ),
        )

    invalid_timestamps = count_invalid(
        payments_df,
        F.col("payment_timestamp").isNull()
        | parsed_timestamp.isNull()
        | (parsed_timestamp > F.current_timestamp()),
    )

    add_check("Payment Timestamp Validity", invalid_timestamps)

    # --------------------------------------------------
    # 10. Load Raw reference datasets
    # --------------------------------------------------
    orders_df = spark.read.parquet(orders_path)
    customers_df = spark.read.parquet(customers_path)

    missing_reference_columns = []

    if "order_id" not in orders_df.columns:
        missing_reference_columns.append("orders.order_id")

    if "customer_id" not in customers_df.columns:
        missing_reference_columns.append("customers.customer_id")

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
    # 11. Duplicate reference keys
    # --------------------------------------------------
    duplicate_order_groups = (
        orders_df
        .filter(F.col("order_id").isNotNull())
        .groupBy("order_id")
        .count()
        .filter(F.col("count") > 1)
    )

    add_check(
        "Duplicate Order Reference IDs",
        duplicate_order_groups.count(),
    )

    duplicate_customer_groups = (
        customers_df
        .filter(F.col("customer_id").isNotNull())
        .groupBy("customer_id")
        .count()
        .filter(F.col("count") > 1)
    )

    add_check(
        "Duplicate Customer Reference IDs",
        duplicate_customer_groups.count(),
    )

    # --------------------------------------------------
    # 12. Order referential integrity
    # --------------------------------------------------
    payment_orders = (
        payments_df
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
        payment_orders
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
    payment_customers = (
        payments_df
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
        payment_customers
        .join(known_customers, on="customer_id", how="left_anti")
        .count()
    )

    add_check(
        "Customer Referential Integrity",
        invalid_customer_references,
    )

    # --------------------------------------------------
    # 14. Overall validation report
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

    print("Payments Validation Summary")

    for check in checks:
        print(
            f"{check['validation_name']}: "
            f"{check['status']} "
            f"(invalid records: {check['invalid_records']})"
        )

    print("Overall Payment Validation Status:", overall_status)

    return report
