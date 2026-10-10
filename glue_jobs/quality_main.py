
import json
import logging
from datetime import datetime, timezone

from pyspark.sql import SparkSession

from quality_customers import run_quality as run_customers_quality
from quality_products import run_quality as run_products_quality
from quality_orders import run_quality as run_orders_quality
from quality_payments import run_quality as run_payments_quality
from quality_events import run_quality as run_events_quality


# ============================================================
# CONFIGURATION
# ============================================================

BUCKET = "ecommerce-data-platform-version1"

RAW_BASE = f"s3://{BUCKET}/raw"
QUALITY_BASE = f"s3://{BUCKET}/processed/quality"

DATASETS = [
    "customers",
    "products",
    "orders",
    "payments",
    "events",
]

LOG_LEVEL = logging.INFO


# ============================================================
# LOGGING
# ============================================================

logging.basicConfig(
    level=LOG_LEVEL,
    format="%(asctime)s | %(levelname)s | %(message)s",
)

logger = logging.getLogger("ECommerceQualityMain")


# ============================================================
# SPARK SESSION
# ============================================================

def create_spark_session():
    return (
        SparkSession.builder
        .appName("ECommerceQualityMain")
        .getOrCreate()
    )


# ============================================================
# DATASET QUALITY RUNNERS
# ============================================================

def run_all_quality_checks(spark):
    """
    Execute quality checks for all five datasets.

    Each module returns a report dictionary containing:
        dataset
        overall_status
        overall_quality_score
        total_records
        metrics
        generated_at
    """

    reports = {}

    customers_path = f"{RAW_BASE}/customers/"
    products_path = f"{RAW_BASE}/products/"
    orders_path = f"{RAW_BASE}/orders/"
    payments_path = f"{RAW_BASE}/payments/"
    events_path = f"{RAW_BASE}/events/"

    # --------------------------------------------------------
    # 1. CUSTOMERS
    # --------------------------------------------------------

    logger.info("Starting CUSTOMERS quality checks")

    try:
        reports["customers"] = run_customers_quality(
            spark=spark,
            customers_path=customers_path,
            quality_report_path=f"{QUALITY_BASE}/customers/",
        )

    except Exception as exc:
        logger.exception("Customers quality checks failed")
        reports["customers"] = {
            "dataset": "customers",
            "overall_status": "FAIL",
            "overall_quality_score": 0.0,
            "total_records": 0,
            "metrics": [],
            "error": str(exc),
        }

    # --------------------------------------------------------
    # 2. PRODUCTS
    # --------------------------------------------------------

    logger.info("Starting PRODUCTS quality checks")

    try:
        reports["products"] = run_products_quality(
            spark=spark,
            products_path=products_path,
            quality_report_path=f"{QUALITY_BASE}/products/",
        )

    except Exception as exc:
        logger.exception("Products quality checks failed")
        reports["products"] = {
            "dataset": "products",
            "overall_status": "FAIL",
            "overall_quality_score": 0.0,
            "total_records": 0,
            "metrics": [],
            "error": str(exc),
        }

    # --------------------------------------------------------
    # 3. ORDERS
    # --------------------------------------------------------

    logger.info("Starting ORDERS quality checks")

    try:
        reports["orders"] = run_orders_quality(
            spark=spark,
            orders_path=orders_path,
            customers_path=customers_path,
            products_path=products_path,
            quality_report_path=f"{QUALITY_BASE}/orders/",
        )

    except Exception as exc:
        logger.exception("Orders quality checks failed")
        reports["orders"] = {
            "dataset": "orders",
            "overall_status": "FAIL",
            "overall_quality_score": 0.0,
            "total_records": 0,
            "metrics": [],
            "error": str(exc),
        }

    # --------------------------------------------------------
    # 4. PAYMENTS
    # --------------------------------------------------------

    logger.info("Starting PAYMENTS quality checks")

    try:
        reports["payments"] = run_payments_quality(
            spark=spark,
            payments_path=payments_path,
            orders_path=orders_path,
            customers_path=customers_path,
            quality_report_path=f"{QUALITY_BASE}/payments/",
        )

    except Exception as exc:
        logger.exception("Payments quality checks failed")
        reports["payments"] = {
            "dataset": "payments",
            "overall_status": "FAIL",
            "overall_quality_score": 0.0,
            "total_records": 0,
            "metrics": [],
            "error": str(exc),
        }

    # --------------------------------------------------------
    # 5. EVENTS
    # --------------------------------------------------------

    logger.info("Starting EVENTS quality checks")

    try:
        reports["events"] = run_events_quality(
            spark=spark,
            events_path=events_path,
            customers_path=customers_path,
            products_path=products_path,
            orders_path=orders_path,
            quality_report_path=f"{QUALITY_BASE}/events/",
        )

    except Exception as exc:
        logger.exception("Events quality checks failed")
        reports["events"] = {
            "dataset": "events",
            "overall_status": "FAIL",
            "overall_quality_score": 0.0,
            "total_records": 0,
            "metrics": [],
            "error": str(exc),
        }

    return reports


# ============================================================
# COMBINED SUMMARY
# ============================================================

def build_summary(reports):
    summary = []

    for dataset in DATASETS:
        report = reports.get(dataset, {})

        summary.append({
            "dataset": dataset,
            "overall_status": report.get(
                "overall_status", "FAIL"
            ),
            "overall_quality_score": float(
                report.get("overall_quality_score", 0.0)
            ),
            "total_records": int(
                report.get("total_records", 0)
            ),
            "error": report.get("error"),
        })

    return summary


def save_summary(spark, summary):
    """
    Save the combined dataset quality summary as JSON
    and Parquet for downstream reporting.
    """

    summary_path = f"{QUALITY_BASE}/quality_summary/"

    summary_timestamp = datetime.now(
        timezone.utc
    ).isoformat()

    summary_records = [
        {
            **item,
            "run_timestamp": summary_timestamp,
        }
        for item in summary
    ]

    summary_df = spark.createDataFrame(summary_records)

    summary_df.write.mode("overwrite").parquet(
        summary_path
    )

    json_path = f"{QUALITY_BASE}/quality_summary_json/"

    # Spark writes a directory containing part JSON files.
    summary_df.write.mode("overwrite").json(
        json_path
    )

    logger.info("Combined Parquet summary saved to %s", summary_path)
    logger.info("Combined JSON summary saved to %s", json_path)

    return summary_records


# ============================================================
# QUALITY GATE
# ============================================================

d
MAX_ALLOWED_FAILED_RECORDS_PER_METRIC = 3


def enforce_quality_gate(reports):
    failed_datasets = []

    for dataset, report in reports.items():
        if report.get("error"):
            failed_datasets.append(dataset)
            continue

        metrics = report.get("metrics", [])

        if not metrics:
            failed_datasets.append(dataset)
            continue

        failed_metrics = [
            metric
            for metric in metrics
            if int(metric.get("failed_records", 0) or 0)
            > MAX_ALLOWED_FAILED_RECORDS_PER_METRIC
        ]

        if failed_metrics:
            failed_datasets.append(dataset)

            print(f"\n{dataset.upper()} FAILED METRICS:")

            for metric in failed_metrics:
                print(
                    f"  {metric['metric_name']}: "
                    f"{metric.get('failed_records', 0)} failed records"
                )

    if failed_datasets:
        raise RuntimeError(
            "QUALITY GATE FAILED. More than 3 failed records "
            "in at least one metric for: "
            + ", ".join(failed_datasets)
        )

    print(
        "\nQUALITY GATE PASSED: Every quality metric across "
        "all five datasets has at most 3 failed records."
    )


# ============================================================
# MAIN
# ============================================================

def main():
    spark = None

    try:
        spark = create_spark_session()

        logger.info("=" * 70)
        logger.info("ECOMMERCE DATA QUALITY PIPELINE STARTED")
        logger.info("=" * 70)

        reports = run_all_quality_checks(spark)

        summary = build_summary(reports)

        logger.info("QUALITY SUMMARY")
        logger.info("-" * 70)

        for item in summary:
            logger.info(
                "%-12s | %-4s | Score: %6.2f%% | Rows: %s",
                item["dataset"],
                item["overall_status"],
                item["overall_quality_score"],
                item["total_records"],
            )

            if item.get("error"):
                logger.error(
                    "%s error: %s",
                    item["dataset"],
                    item["error"],
                )

        save_summary(spark, summary)

        # This raises an exception if even one dataset fails.
        # Put downstream transformations after this gate.
        enforce_quality_gate(summary)

        logger.info(
            "All quality checks passed. "
            "The pipeline may proceed to downstream transformations."
        )

    except Exception:
        logger.exception("ECOMMERCE DATA QUALITY PIPELINE FAILED")
        raise

    finally:
        if spark is not None:
            spark.stop()
            logger.info("Spark session stopped")


if __name__ == "__main__":
    main()
