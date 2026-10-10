
from datetime import datetime, timezone

from pyspark.sql import functions as F


# ============================================================
# CUSTOMER DATA QUALITY
# ============================================================

DATASET_NAME = "customers"

QUALITY_COLUMNS = [
    "customer_id",
    "name",
    "email",
    "city",
    "state",
    "country",
    "signup_date",
]

EXPECTED_COLUMNS = set(QUALITY_COLUMNS)

CUSTOMER_ID_PATTERN = r"^C[0-9]{5}$"

EMAIL_PATTERN = (
    r"^[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\.[A-Za-z]{2,}$"
)

VALID_COUNTRIES = [
    "Australia",
    "India",
    "United States",
    "United Kingdom",
    "Canada",
]

VALID_STATES = [
    "NSW",
    "VIC",
    "QLD",
    "WA",
    "SA",
]

# Signup dates in the source generator use dd-MM-yyyy.
SIGNUP_DATE_FORMAT = "dd-MM-yyyy"


# ============================================================
# HELPERS
# ============================================================

def percentage(numerator, denominator):
    """Return a percentage safely when the denominator is zero."""
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
    """Add a consistent metric record to the report."""

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
    customers_path,
    quality_report_path=None,
):
    """
    Run customer data quality checks against the Raw Parquet layer.

    Parameters
    ----------
    spark:
        Existing SparkSession supplied by quality_main.py.
    customers_path:
        S3 or local path to Raw customers Parquet.
    quality_report_path:
        Optional path for the persisted Parquet quality report.

    Returns
    -------
    dict:
        Dataset quality summary and detailed metric records.
    """

    run_timestamp = datetime.now(timezone.utc).isoformat()

    print("=" * 70)
    print("CUSTOMER DATA QUALITY")
    print("=" * 70)
    print("Input path:", customers_path)

    customers_df = spark.read.parquet(customers_path)

    # Normalize whitespace for string columns.
    for field in customers_df.schema.fields:
        if field.dataType.simpleString() == "string":
            customers_df = customers_df.withColumn(
                field.name,
                F.trim(F.col(field.name)),
            )

    actual_columns = set(customers_df.columns)

    missing_columns = sorted(
        EXPECTED_COLUMNS - actual_columns
    )

    unexpected_columns = sorted(
        actual_columns - EXPECTED_COLUMNS
    )

    if missing_columns:
        raise ValueError(
            "Customer dataset is missing required columns: "
            + ", ".join(missing_columns)
        )

    total_records = customers_df.count()

    print("Total customer records:", total_records)
    customers_df.printSchema()

    if total_records == 0:
        raise ValueError(
            "Customer dataset is empty; quality score cannot be calculated."
        )

    # Parse signup_date explicitly without changing the stored source column.
    customers_df = customers_df.withColumn(
        "_parsed_signup_date",
        F.coalesce(
            F.to_date(
                F.col("signup_date"),
                SIGNUP_DATE_FORMAT,
            ),
            F.to_date(
                F.col("signup_date"),
                "yyyy-MM-dd",
            ),
        ),
    )

    # ========================================================
    # 1. COMPLETENESS
    # ========================================================

    print("\n1. CUSTOMER COMPLETENESS")

    metrics = []

    completeness_expressions = []

    for column_name in QUALITY_COLUMNS:
        completeness_expressions.append(
            F.sum(
                F.when(
                    F.col(column_name).isNotNull()
                    & (F.trim(F.col(column_name)) != ""),
                    1,
                ).otherwise(0)
            ).alias(column_name)
        )

    completeness_row = customers_df.agg(
        *completeness_expressions
    ).first()

    completeness_scores = []

    for column_name in QUALITY_COLUMNS:
        non_missing = int(
            completeness_row[column_name] or 0
        )

        score = percentage(non_missing, total_records)
        completeness_scores.append(score)

        print(
            f"{column_name:<18} "
            f"Complete: {non_missing:,}/{total_records:,} "
            f"({score:.2f}%)"
        )

        add_metric(
            metrics,
            f"Completeness - {column_name}",
            non_missing,
            total_records,
            f"Non-null and non-blank {column_name} values.",
        )

    overall_completeness = percentage(
        sum(completeness_scores),
        len(completeness_scores),
    )

    # ========================================================
    # 2. CUSTOMER ID UNIQUENESS
    # ========================================================

    print("\n2. CUSTOMER ID UNIQUENESS")

    unique_id_count = customers_df.select(
        "customer_id"
    ).distinct().count()

    duplicate_id_groups = (
        customers_df
        .filter(
            F.col("customer_id").isNotNull()
            & (F.col("customer_id") != "")
        )
        .groupBy("customer_id")
        .count()
        .filter(F.col("count") > 1)
        .count()
    )

    # Record-level uniqueness score excludes duplicate ID occurrences.
    duplicate_id_rows = (
        customers_df
        .filter(
            F.col("customer_id").isNotNull()
            & (F.col("customer_id") != "")
        )
        .groupBy("customer_id")
        .count()
        .filter(F.col("count") > 1)
        .agg(
            F.sum("count").alias("duplicate_rows")
        )
        .first()["duplicate_rows"]
        or 0
    )

    nonblank_id_rows = customers_df.filter(
        F.col("customer_id").isNotNull()
        & (F.col("customer_id") != "")
    ).count()

    duplicate_excess_rows = max(
        int(duplicate_id_rows) - int(duplicate_id_groups),
        0,
    )

    unique_record_count = max(
        nonblank_id_rows - duplicate_excess_rows,
        0,
    )

    uniqueness_score = percentage(
        unique_record_count,
        total_records,
    )

    print("Distinct customer IDs:", unique_id_count)
    print("Duplicate ID groups:", duplicate_id_groups)
    print("Uniqueness score:", f"{uniqueness_score:.2f}%")

    add_metric(
        metrics,
        "Customer ID Uniqueness",
        unique_record_count,
        total_records,
        "Record-level uniqueness; duplicate IDs reduce the score.",
    )

    # ========================================================
    # 3. CUSTOMER ID FORMAT
    # ========================================================

    print("\n3. CUSTOMER ID FORMAT")

    valid_id_count = customers_df.filter(
        F.col("customer_id").rlike(CUSTOMER_ID_PATTERN)
    ).count()

    id_format_score = percentage(
        valid_id_count,
        total_records,
    )

    print("Valid customer IDs:", valid_id_count)
    print("ID format score:", f"{id_format_score:.2f}%")

    add_metric(
        metrics,
        "Customer ID Format",
        valid_id_count,
        total_records,
        "IDs must match C followed by exactly five digits.",
    )

    # ========================================================
    # 4. EMAIL VALIDITY
    # ========================================================

    print("\n4. EMAIL VALIDITY")

    valid_email_count = customers_df.filter(
        F.col("email").rlike(EMAIL_PATTERN)
    ).count()

    email_score = percentage(
        valid_email_count,
        total_records,
    )

    print("Valid email records:", valid_email_count)
    print("Email validity:", f"{email_score:.2f}%")

    add_metric(
        metrics,
        "Email Validity",
        valid_email_count,
        total_records,
        "Email must match the configured basic email pattern.",
    )

    # ========================================================
    # 5. COUNTRY VALIDITY
    # ========================================================

    print("\n5. COUNTRY VALIDITY")

    valid_country_count = customers_df.filter(
        F.col("country").isin(VALID_COUNTRIES)
    ).count()

    country_score = percentage(
        valid_country_count,
        total_records,
    )

    print("Recognized country records:", valid_country_count)
    print("Country validity:", f"{country_score:.2f}%")

    add_metric(
        metrics,
        "Country Validity",
        valid_country_count,
        total_records,
        "Country must be in the configured allowed-country list.",
    )

    # ========================================================
    # 6. CITY AND STATE VALIDITY
    # ========================================================

    print("\n6. CITY AND STATE VALIDITY")

    city_nonblank_count = customers_df.filter(
        F.col("city").isNotNull()
        & (F.col("city") != "")
    ).count()

    # State validation is specific to the Australian sample data.
    # For other countries, the state rule should be configured
    # according to the country-specific business requirements.
    valid_state_count = customers_df.filter(
        (
            F.col("country") == "Australia"
        )
        & F.col("state").isin(VALID_STATES)
    ).count()

    city_score = percentage(
        city_nonblank_count,
        total_records,
    )

    state_score = percentage(
        valid_state_count,
        total_records,
    )

    print("Nonblank city records:", city_nonblank_count)
    print("City completeness:", f"{city_score:.2f}%")
    print("Valid Australian state records:", valid_state_count)
    print("State validity:", f"{state_score:.2f}%")

    add_metric(
        metrics,
        "City Completeness",
        city_nonblank_count,
        total_records,
        "City must not be null or blank.",
    )

    add_metric(
        metrics,
        "State Validity",
        valid_state_count,
        total_records,
        "Australian customers must have an allowed state code.",
    )

    # ========================================================
    # 7. SIGNUP DATE VALIDITY
    # ========================================================

    print("\n7. SIGNUP DATE VALIDITY")

    valid_signup_count = customers_df.filter(
        F.col("_parsed_signup_date").isNotNull()
        & (
            F.col("_parsed_signup_date")
            <= F.current_date()
        )
    ).count()

    signup_date_score = percentage(
        valid_signup_count,
        total_records,
    )

    print("Valid signup dates:", valid_signup_count)
    print("Signup date validity:", f"{signup_date_score:.2f}%")

    add_metric(
        metrics,
        "Signup Date Validity",
        valid_signup_count,
        total_records,
        "Signup date must parse successfully and not be in the future.",
    )

    # ========================================================
    # 8. DUPLICATE RECORDS
    # ========================================================

    print("\n8. DUPLICATE RECORDS")

    duplicate_record_groups = (
        customers_df
        .select(*QUALITY_COLUMNS)
        .groupBy(*QUALITY_COLUMNS)
        .count()
        .filter(F.col("count") > 1)
        .count()
    )

    duplicate_record_rows = (
        customers_df
        .select(*QUALITY_COLUMNS)
        .groupBy(*QUALITY_COLUMNS)
        .count()
        .filter(F.col("count") > 1)
        .agg(
            F.sum("count").alias("duplicate_rows")
        )
        .first()["duplicate_rows"]
        or 0
    )

    duplicate_record_excess = max(
        int(duplicate_record_rows) - int(duplicate_record_groups),
        0,
    )

    unique_full_record_count = max(
        total_records - duplicate_record_excess,
        0,
    )

    duplicate_record_score = percentage(
        unique_full_record_count,
        total_records,
    )

    print("Duplicate full-record groups:", duplicate_record_groups)
    print(
        "Full-record uniqueness:",
        f"{duplicate_record_score:.2f}%",
    )

    add_metric(
        metrics,
        "Full Record Uniqueness",
        unique_full_record_count,
        total_records,
        "Identical records reduce the score.",
    )

    # ========================================================
    # 9. OVERALL QUALITY SCORE
    # ========================================================

    print("\n9. OVERALL CUSTOMER QUALITY SCORE")

    # The overall score is the unweighted average of metric scores.
    # Each metric uses the actual data, not a hardcoded score.
    overall_quality_score = round(
        sum(
            metric["metric_score"]
            for metric in metrics
        ) / len(metrics),
        2,
    )

    print("Overall customer quality:", f"{overall_quality_score:.2f}%")

    # Missing/unexpected schema information is included in the report.
    overall_status = (
        "PASS"
        if overall_quality_score >= 95.0
        and not missing_columns
        else "FAIL"
    )

    # ========================================================
    # 10. QUALITY REPORT
    # ========================================================

    report_records = []

    for metric in metrics:
        report_records.append({
            **metric,
            "overall_quality_score": overall_quality_score,
            "overall_status": overall_status,
            "run_timestamp": run_timestamp,
            "missing_columns": ",".join(missing_columns),
            "unexpected_columns": ",".join(unexpected_columns),
        })

    report = {
        "dataset": DATASET_NAME,
        "overall_status": overall_status,
        "overall_quality_score": overall_quality_score,
        "total_records": total_records,
        "metrics": metrics,
        "missing_columns": missing_columns,
        "unexpected_columns": unexpected_columns,
        "generated_at": run_timestamp,
    }

    print("\nQuality metrics:")
    for metric in metrics:
        print(
            f"{metric['metric_name']:<32} "
            f"{metric['metric_score']:.2f}%"
        )

    print("Overall status:", overall_status)

    # ========================================================
    # 11. PERSIST QUALITY REPORT
    # ========================================================

    if quality_report_path:
        report_df = spark.createDataFrame(report_records)

        (
            report_df.write
            .mode("overwrite")
            .parquet(quality_report_path)
        )

        print(
            "Quality report saved to:",
            quality_report_path,
        )

        saved_report = spark.read.parquet(
            quality_report_path
        )

        print("Persisted quality report:")
        saved_report.show(truncate=False)

    return report
