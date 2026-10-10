
from datetime import datetime, timezone

from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import DateType, TimestampType


def validate(spark: SparkSession, bucket: str) -> dict:
    """Strict validation of Raw Orders using AWS Glue."""

    dataset = "orders"

    orders_path = f"s3://{bucket}/raw/orders/"
    customers_path = f"s3://{bucket}/raw/customers/"
    products_path = f"s3://{bucket}/raw/products/"

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

    def blank(column_name):
        value = F.col(column_name)
        return (
            value.isNull()
            | (F.length(F.trim(value.cast("string"))) == 0)
        )

    # --------------------------------------------------
    # 1. Read Raw Orders
    # --------------------------------------------------
    orders_df = spark.read.parquet(orders_path)
    row_count = orders_df.count()

    print("Raw Orders loaded successfully")
    print("Order count:", row_count)
    orders_df.printSchema()

    required_columns = [
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

    missing_columns = [
        name for name in required_columns
        if name not in orders_df.columns
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
    # --------------------------------------------------
    for name in required_columns:
        add_check(
            f"Required Field: {name}",
            count_invalid(orders_df, blank(name)),
        )

    # --------------------------------------------------
    # 3. Duplicate Order IDs
    # Count extra duplicate rows, not duplicate groups.
    # --------------------------------------------------
    duplicate_order_groups = (
        orders_df
        .filter(F.col("order_id").isNotNull())
        .groupBy("order_id")
        .count()
        .filter(F.col("count") > 1)
    )

    duplicate_order_count = (
        duplicate_order_groups
        .agg(
            F.coalesce(
                F.sum(F.col("count") - 1), F.lit(0)
            ).alias("invalid")
        )
        .first()["invalid"]
    )

    add_check("Duplicate Order IDs", duplicate_order_count)

    # --------------------------------------------------
    # 4. Numeric business rules
    # Null values are handled by required-field checks.
    # They are also excluded from these specific checks.
    # --------------------------------------------------
    quantity = F.col("quantity").cast("decimal(20,4)")
    unit_price = F.col("unit_price").cast("decimal(20,4)")
    order_amount = F.col("order_amount").cast("decimal(20,2)")

    add_check(
        "Quantity Must Be Positive",
        count_invalid(
            orders_df,
            F.col("quantity").isNotNull()
            & (
                quantity.isNull()
                | (quantity <= 0)
            ),
        ),
    )

    add_check(
        "Unit Price Must Be Non-Negative",
        count_invalid(
            orders_df,
            F.col("unit_price").isNotNull()
            & (
                unit_price.isNull()
                | (unit_price < 0)
            ),
        ),
    )

    add_check(
        "Order Amount Must Be Non-Negative",
        count_invalid(
            orders_df,
            F.col("order_amount").isNotNull()
            & (
                order_amount.isNull()
                | (order_amount < 0)
            ),
        ),
    )

    # --------------------------------------------------
    # 5. Order amount consistency
    # Expected amount = quantity * unit_price,
    # rounded to 2 decimal places.
    # --------------------------------------------------
    expected_amount = F.round(
        quantity * unit_price, 2
    ).cast("decimal(20,2)")

    invalid_amount_calculations = count_invalid(
        orders_df,
        quantity.isNull()
        | unit_price.isNull()
        | order_amount.isNull()
        | (
            order_amount
            != expected_amount
        ),
    )

    add_check(
        "Order Amount Calculation",
        invalid_amount_calculations,
    )

    # --------------------------------------------------
    # 6. Allowed order statuses
    # --------------------------------------------------
    valid_order_statuses = [
        "COMPLETED",
        "SHIPPED",
        "PROCESSING",
        "CANCELLED",
        "RETURNED",
    ]

    add_check(
        "Order Status Validity",
        count_invalid(
            orders_df,
            F.col("order_status").isNull()
            | ~F.col("order_status").isin(valid_order_statuses),
        ),
    )

    # --------------------------------------------------
    # 7. Allowed payment statuses
    # --------------------------------------------------
    valid_payment_statuses = [
        "PAID",
        "PENDING",
        "FAILED",
        "REFUNDED",
    ]

    add_check(
        "Payment Status Validity",
        count_invalid(
            orders_df,
            F.col("payment_status").isNull()
            | ~F.col("payment_status").isin(valid_payment_statuses),
        ),
    )

    # --------------------------------------------------
    # 8. Order timestamp validity
    # --------------------------------------------------
    timestamp_type = orders_df.schema["order_timestamp"].dataType

    if isinstance(timestamp_type, (DateType, TimestampType)):
        parsed_timestamp = F.col("order_timestamp").cast("timestamp")
    else:
        parsed_timestamp = F.coalesce(
            F.to_timestamp(F.col("order_timestamp")),
            F.to_timestamp(
                F.col("order_timestamp"),
                "yyyy-MM-dd'T'HH:mm:ss.SSSXXX",
            ),
            F.to_timestamp(
                F.col("order_timestamp"),
                "yyyy-MM-dd HH:mm:ss",
            ),
        )

    add_check(
        "Order Timestamp Validity",
        count_invalid(
            orders_df,
            F.col("order_timestamp").isNull()
            | parsed_timestamp.isNull()
            | (parsed_timestamp > F.current_timestamp()),
        ),
    )

    # --------------------------------------------------
    # 9. Load Raw reference datasets
    # --------------------------------------------------
    customers_df = spark.read.parquet(customers_path)
    products_df = spark.read.parquet(products_path)

    reference_keys = [
        (customers_df, "customer_id", "Customers Reference Schema"),
        (products_df, "product_id", "Products Reference Schema"),
    ]

    missing_reference_columns = []

    for reference_df, key, check_name in reference_keys:
        if key not in reference_df.columns:
            missing_reference_columns.append(key)

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
    # 10. Reference dataset key quality
    # Duplicate reference keys should fail validation.
    # --------------------------------------------------
    duplicate_customer_groups = (
        customers_df
        .filter(F.col("customer_id").isNotNull())
        .groupBy("customer_id")
        .count()
        .filter(F.col("count") > 1)
    )

    duplicate_customer_count = duplicate_customer_groups.count()

    add_check(
        "Duplicate Customer Reference IDs",
        duplicate_customer_count,
    )

    duplicate_product_groups = (
        products_df
        .filter(F.col("product_id").isNotNull())
        .groupBy("product_id")
        .count()
        .filter(F.col("count") > 1)
    )

    duplicate_product_count = duplicate_product_groups.count()

    add_check(
        "Duplicate Product Reference IDs",
        duplicate_product_count,
    )

    # --------------------------------------------------
    # 11. Customer referential integrity
    # --------------------------------------------------
    order_customers = (
        orders_df
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

    invalid_customer_count = (
        order_customers
        .join(
            known_customers,
            on="customer_id",
            how="left_anti",
        )
        .count()
    )

    add_check(
        "Customer Referential Integrity",
        invalid_customer_count,
    )

    # --------------------------------------------------
    # 12. Product referential integrity
    # --------------------------------------------------
    order_products = (
        orders_df
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

    invalid_product_count = (
        order_products
        .join(
            known_products,
            on="product_id",
            how="left_anti",
        )
        .count()
    )

    add_check(
        "Product Referential Integrity",
        invalid_product_count,
    )

    # --------------------------------------------------
    # 13. Overall report
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

    print("Orders Validation Summary")

    for check in checks:
        print(
            f"{check['validation_name']}: "
            f"{check['status']} "
            f"(invalid records: {check['invalid_records']})"
        )

    print("Overall Orders Validation Status:", overall_status)

    return report
