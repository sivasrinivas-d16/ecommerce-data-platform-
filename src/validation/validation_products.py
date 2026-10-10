
from datetime import datetime, timezone

from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import DateType, TimestampType


def validate(spark: SparkSession, bucket: str) -> dict:
    """Strict validation of Raw Products using AWS Glue."""

    dataset = "products"
    products_path = f"s3://{bucket}/raw/products/"

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

    def blank(name):
        value = F.col(name)
        return (
            value.isNull()
            | (F.length(F.trim(value.cast("string"))) == 0)
        )

    # --------------------------------------------------
    # 1. Load Raw Products
    # --------------------------------------------------
    products_df = spark.read.parquet(products_path)
    row_count = products_df.count()

    print("Products loaded from Raw layer")
    print("Total product records:", row_count)
    products_df.printSchema()

    required_columns = [
        "product_id",
        "product_name",
        "category",
        "brand",
        "price",
        "stock_quantity",
        "product_status",
        "created_date",
    ]

    missing_columns = [
        name for name in required_columns
        if name not in products_df.columns
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
    # Reject null, empty, and whitespace-only values.
    # --------------------------------------------------
    for name in required_columns:
        add_check(
            f"Required Field: {name}",
            count_invalid(products_df, blank(name)),
        )

    # --------------------------------------------------
    # 3. Product ID format
    # Expected: P followed by exactly 6 digits.
    # --------------------------------------------------
    invalid_product_ids = count_invalid(
        products_df,
        F.col("product_id").isNull()
        | ~F.col("product_id").rlike(r"^P[0-9]{6}$"),
    )

    add_check("Product ID Format", invalid_product_ids)

    # --------------------------------------------------
    # 4. Product ID uniqueness
    # Count duplicate rows beyond the first occurrence.
    # --------------------------------------------------
    duplicate_groups = (
        products_df
        .filter(F.col("product_id").isNotNull())
        .groupBy("product_id")
        .count()
        .filter(F.col("count") > 1)
    )

    duplicate_product_count = (
        duplicate_groups
        .agg(
            F.coalesce(
                F.sum(F.col("count") - 1), F.lit(0)
            ).alias("invalid")
        )
        .first()["invalid"]
    )

    add_check("Duplicate Product IDs", duplicate_product_count)

    # --------------------------------------------------
    # 5. Product name, category, brand
    # Required-field checks already catch blank values.
    # These checks reject surrounding whitespace.
    # --------------------------------------------------
    for name in ["product_name", "category", "brand"]:
        add_check(
            f"{name} Whitespace Validation",
            count_invalid(
                products_df,
                F.col(name).isNotNull()
                & (F.col(name) != F.trim(F.col(name))),
            ),
        )

    # --------------------------------------------------
    # 6. Price validation
    # Preserve original rule: price must be >= 0.
    # Also reject non-numeric values.
    # --------------------------------------------------
    price = F.col("price").cast("decimal(20,4)")

    invalid_price_count = count_invalid(
        products_df,
        F.col("price").isNotNull()
        & (
            price.isNull()
            | (price < 0)
        ),
    )

    add_check("Product Price Validity", invalid_price_count)

    # --------------------------------------------------
    # 7. Stock quantity validation
    # Stock must be a whole number >= 0.
    # --------------------------------------------------
    stock = F.col("stock_quantity").cast("decimal(20,4)")

    invalid_stock_count = count_invalid(
        products_df,
        F.col("stock_quantity").isNotNull()
        & (
            stock.isNull()
            | (stock < 0)
            | (stock != F.floor(stock))
        ),
    )

    add_check("Stock Quantity Validity", invalid_stock_count)

    # --------------------------------------------------
    # 8. Product status validation
    # --------------------------------------------------
    valid_product_statuses = [
        "ACTIVE",
        "INACTIVE",
        "DISCONTINUED",
    ]

    invalid_status_count = count_invalid(
        products_df,
        F.col("product_status").isNull()
        | ~F.col("product_status").isin(valid_product_statuses),
    )

    add_check("Product Status Validity", invalid_status_count)

    # --------------------------------------------------
    # 9. Created date validation
    # Reject unparseable and future dates.
    # --------------------------------------------------
    date_type = products_df.schema["created_date"].dataType

    if isinstance(date_type, (DateType, TimestampType)):
        parsed_date = F.to_date(F.col("created_date"))
    else:
        parsed_date = F.coalesce(
            F.to_date(F.col("created_date"), "yyyy-MM-dd"),
            F.to_date(F.col("created_date"), "yyyy-MM-dd HH:mm:ss"),
            F.to_date(F.col("created_date"), "yyyy/MM/dd"),
        )

    invalid_created_date_count = count_invalid(
        products_df,
        F.col("created_date").isNull()
        | parsed_date.isNull()
        | (parsed_date > F.current_date()),
    )

    add_check("Created Date Validity", invalid_created_date_count)

    # --------------------------------------------------
    # 10. Overall validation report
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

    print("Products Validation Summary")

    for check in checks:
        print(
            f"{check['validation_name']}: "
            f"{check['status']} "
            f"(invalid records: {check['invalid_records']})"
        )

    print("Overall Product Validation Status:", overall_status)

    return report
