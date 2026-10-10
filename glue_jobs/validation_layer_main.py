
import sys
import json
import boto3

from datetime import datetime, timezone
from pyspark.sql import SparkSession
from awsglue.utils import getResolvedOptions

# ---------------------------------------------------------
# 1. Glue Job Configuration
# ---------------------------------------------------------

args = getResolvedOptions(
    sys.argv,
    ["JOB_NAME", "S3_BUCKET"]
)

BUCKET = args["S3_BUCKET"]

REPORT_PREFIX = "processed/validation"

s3 = boto3.client("s3")

# AWS Glue manages this Spark session.
spark = SparkSession.builder.getOrCreate()


# ---------------------------------------------------------
# 2. Import Existing Validation Modules
# ---------------------------------------------------------

from validate_customers import validate as validate_customers
from validate_products import validate as validate_products
from validate_orders import validate as validate_orders
from validate_payments import validate as validate_payments
from validate_events import validate as validate_events


# ---------------------------------------------------------
# 3. Validation Execution Order
# ---------------------------------------------------------

VALIDATORS = [
    ("customers", validate_customers),
    ("products", validate_products),
    ("orders", validate_orders),
    ("payments", validate_payments),
    ("events", validate_events),
]


# ---------------------------------------------------------
# 4. Write JSON Report to S3
# ---------------------------------------------------------

def write_report(dataset, report):
    """
    Write one validation report per dataset.
    Raises an exception if S3 report persistence fails.
    """

    key = f"{REPORT_PREFIX}/{dataset}/validation.json"

    report["dataset"] = dataset
    report["generated_at"] = datetime.now(
        timezone.utc
    ).isoformat()

    report_json = json.dumps(
        report,
        indent=2,
        default=str
    )

    s3.put_object(
        Bucket=BUCKET,
        Key=key,
        Body=report_json.encode("utf-8"),
        ContentType="application/json"
    )

    print(f"Report saved: s3://{BUCKET}/{key}")


# ---------------------------------------------------------
# 5. Run All Dataset Validations
# ---------------------------------------------------------

def main():
    failed_datasets = []

    for dataset, validator in VALIDATORS:

        print("\n" + "=" * 70)
        print(f"STARTING VALIDATION: {dataset.upper()}")
        print("=" * 70)

        try:
            # Run the existing dataset validation logic.
            report = validator(spark, BUCKET)

            if not isinstance(report, dict):
                raise TypeError(
                    f"{dataset} validator must return a dictionary"
                )

            # Missing status is a failure.
            status = str(
                report.get("overall_status", "FAIL")
            ).upper()

            if status != "PASS":
                report["overall_status"] = "FAIL"
                failed_datasets.append(dataset)
            else:
                report["overall_status"] = "PASS"

            # Persist report even when validation returns FAIL.
            write_report(dataset, report)

            print(
                f"{dataset.upper()} validation status: "
                f"{report['overall_status']}"
            )

        except Exception as exc:
            print(f"ERROR validating {dataset}: {exc}")

            failed_datasets.append(dataset)

            # Attempt to persist the failure report.
            # If this write fails, the Glue job also fails.
            error_report = {
                "overall_status": "FAIL",
                "error": str(exc),
                "run_timestamp": datetime.now(
                    timezone.utc
                ).isoformat(),
            }

            write_report(dataset, error_report)

    # -----------------------------------------------------
    # 6. Final Pipeline Gate
    # -----------------------------------------------------

    print("\n" + "=" * 70)
    print("FINAL VALIDATION SUMMARY")
    print("=" * 70)

    if failed_datasets:
        failed_datasets = sorted(set(failed_datasets))

        print("Failed datasets:", failed_datasets)

        raise RuntimeError(
            "Validation pipeline FAILED. "
            "Transformation must not start. Failed datasets: "
            + ", ".join(failed_datasets)
        )

    print("All five datasets passed validation.")
    print("Validation pipeline completed successfully.")


# ---------------------------------------------------------
# 7. Main Entry Point
# ---------------------------------------------------------

if __name__ == "__main__":
    try:
        main()
    finally:
        spark.stop()
