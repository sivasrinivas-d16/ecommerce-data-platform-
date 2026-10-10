
from datetime import datetime, timezone

from pyspark.sql import functions as F


# ============================================================
# PRODUCTS DATA QUALITY CONFIGURATION
# ============================================================

DATASET_NAME = "products"

QUALITY_COLUMNS = [
    "product_id",
    "product_name",
    "category",
    "subcategory",
    "brand",
    "price",
    "stock_quantity",
    "product_status",
    "created_date",
]

PRODUCT_ID_PATTERN = r"^P[0-9]{6}$"

VALID_PRODUCT_STATUSES = [
    "ACTIVE",
    "INACTIVE",
    "DISCONTINUED",
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
    Calculate duplicate groups and excess duplicate rows.
    Null and blank values are handled by completeness checks.
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
    products_path,
    quality_report_path=None,
):
    """
    Run product data quality checks.

    Uses the Spark session supplied by the caller.
    Returns a dictionary for quality_main.py.
    """

    run_timestamp = datetime.now(timezone.utc).isoformat()

    print("=" * 70)
    print("PRODUCTS DATA QUALITY")
    print("=" * 70)
    print("Products path:", products_path)

    products_df = spark.read.parquet(products_path)

    # Normalize string values before checking them.
    for field in products_df.schema.fields:
        if field.dataType.simpleString() == "string":
            products_df = products_df.withColumn(
                field.name,
                F.trim(F.col(field.name)),
            )

    missing_columns = sorted(
        set(QUALITY_COLUMNS) - set(products_df.columns)
    )

    if missing_columns:
        raise ValueError(
            "Products dataset is missing required columns: "
            + ", ".join(missing_columns)
        )

    total_records = products_df.count()

    print("Total product records:", total_records)
    products_df.printSchema()

    if total_records == 0:
        raise ValueError(
            "Products dataset is empty; quality score cannot be calculated."
        )

    metrics = []

    # ========================================================
    # 1. COMPLETENESS
    # ========================================================

    print("\n1. PRODUCT COMPLETENESS")

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

    completeness_row = products_df.agg(
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
    # 2. PRODUCT ID UNIQUENESS
    # ========================================================

    print("\n2. PRODUCT ID UNIQUENESS")

    duplicate_groups, duplicate_excess = duplicate_statistics(
        products_df,
        "product_id",
    )

    unique_product_records = max(
        total_records - duplicate_excess,
        0,
    )

    product_id_uniqueness = percentage(
        unique_product_records,
        total_records,
    )

    print("Duplicate product ID groups:", duplicate_groups)
    print("Product ID uniqueness:", f"{product_id_uniqueness:.2f}%")

    add_metric(
        metrics,
        "Product ID Uniqueness",
        unique_product_records,
        total_records,
        "Duplicate product IDs reduce the uniqueness score.",
    )

    # ========================================================
    # 3. PRODUCT ID FORMAT
    # ========================================================

    valid_product_id_count = products_df.filter(
        F.col("product_id").rlike(PRODUCT_ID_PATTERN)
    ).count()

    product_id_validity = percentage(
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
    # 4. PRODUCT NAME VALIDITY
    # ========================================================

    valid_product_name_count = products_df.filter(
        F.col("product_name").isNotNull()
        & (F.col("product_name") != "")
    ).count()

    product_name_validity = percentage(
        valid_product_name_count,
        total_records,
    )

    add_metric(
        metrics,
        "Product Name Validity",
        valid_product_name_count,
        total_records,
        "Product names must not be null or blank.",
    )

    # ========================================================
    # 5. CATEGORY AND SUBCATEGORY VALIDITY
    # ========================================================

    valid_category_count = products_df.filter(
        F.col("category").isNotNull()
        & (F.col("category") != "")
    ).count()

    category_validity = percentage(
        valid_category_count,
        total_records,
    )

    add_metric(
        metrics,
        "Category Validity",
        valid_category_count,
        total_records,
        "Category must not be null or blank.",
    )

    valid_subcategory_count = products_df.filter(
        F.col("subcategory").isNotNull()
        & (F.col("subcategory") != "")
    ).count()

    subcategory_validity = percentage(
        valid_subcategory_count,
        total_records,
    )

    add_metric(
        metrics,
        "Subcategory Validity",
        valid_subcategory_count,
        total_records,
        "Subcategory must not be null or blank.",
    )

    # ========================================================
    # 6. BRAND VALIDITY
    # ========================================================

    valid_brand_count = products_df.filter(
        F.col("brand").isNotNull()
        & (F.col("brand") != "")
    ).count()

    brand_validity = percentage(
        valid_brand_count,
        total_records,
    )

    add_metric(
        metrics,
        "Brand Validity",
        valid_brand_count,
        total_records,
        "Brand must not be null or blank.",
    )

    # ========================================================
    # 7. PRICE VALIDITY
    # ========================================================

    valid_price_count = products_df.filter(
        F.col("price").isNotNull()
        & (F.col("price") >= 0)
    ).count()

    price_validity = percentage(
        valid_price_count,
        total_records,
    )

    add_metric(
        metrics,
        "Price Validity",
        valid_price_count,
        total_records,
        "Price must be present and greater than or equal to zero.",
    )

    # ========================================================
    # 8. STOCK QUANTITY VALIDITY
    # ========================================================

    valid_stock_count = products_df.filter(
        F.col("stock_quantity").isNotNull()
        & (F.col("stock_quantity") >= 0)
        & (
            F.col("stock_quantity")
            == F.floor(F.col("stock_quantity"))
        )
    ).count()

    stock_quantity_validity = percentage(
        valid_stock_count,
        total_records,
    )

    add_metric(
        metrics,
        "Stock Quantity Validity",
        valid_stock_count,
        total_records,
        "Stock must be a non-negative whole number.",
    )

    # ========================================================
    # 9. PRODUCT STATUS VALIDITY
    # ========================================================

    valid_product_status_count = products_df.filter(
        F.col("product_status").isin(
            VALID_PRODUCT_STATUSES
        )
    ).count()

    product_status_validity = percentage(
        valid_product_status_count,
        total_records,
    )

    add_metric(
        metrics,
        "Product Status Validity",
        valid_product_status_count,
        total_records,
        "Product status must be ACTIVE, INACTIVE, or DISCONTINUED.",
    )

    # ========================================================
    # 10. CREATED DATE VALIDITY
    # ========================================================

    created_date = F.to_date(
        F.col("created_date"),
        "yyyy-MM-dd",
    )

    valid_created_date_count = products_df.filter(
        F.col("created_date").isNotNull()
        & (F.col("created_date") != "")
        & created_date.isNotNull()
        & (created_date <= F.current_date())
    ).count()

    created_date_validity = percentage(
        valid_created_date_count,
        total_records,
    )

    add_metric(
        metrics,
        "Created Date Validity",
        valid_created_date_count,
        total_records,
        "Created date must be a valid ISO date and not in the future.",
    )

    # ========================================================
    # 11. OVERALL QUALITY SCORE
    # ========================================================

    overall_quality_score = round(
        sum(metric["metric_score"] for metric in metrics)
        / len(metrics),
        2,
    )

    overall_status = (
        "PASS"
        if overall_quality_score >= 95.0
        and product_id_uniqueness == 100.0
        and product_id_validity == 100.0
        and product_status_validity == 100.0
        else "FAIL"
    )

    print("\n" + "=" * 70)
    print("PRODUCTS QUALITY SUMMARY")
    print("=" * 70)

    for metric in metrics:
        print(
            f"{metric['metric_name']:<40}"
            f"{metric['metric_score']:>8.2f}%"
        )

    print("Overall quality score:", f"{overall_quality_score:.2f}%")
    print("Overall status:", overall_status)

    # ========================================================
    # 12. QUALITY REPORT
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
