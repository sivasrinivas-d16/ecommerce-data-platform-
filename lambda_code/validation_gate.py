
import json
import logging
import re
from collections import defaultdict

import boto3
from botocore.exceptions import ClientError

logger = logging.getLogger()
logger.setLevel(logging.INFO)

s3 = boto3.client("s3")

BUCKET = "ecommerce-data-platform-version1"

VALIDATION_PREFIX = "processed/validation/"
QUALITY_PREFIX = "processed/quality/quality_summary_json/"

DATASETS = {
    "customers",
    "products",
    "orders",
    "payments",
    "events",
}

MAX_FAILED_COLUMNS = 4

# Configure these names to match the actual columns in each dataset.
# They are used to identify columns referenced by failed validation checks.
COLUMN_NAMES = {
    "customers": [
        "customer_id", "first_name", "last_name", "email",
        "phone", "city", "state", "country", "signup_date"
    ],
    "products": [
        "product_id", "product_name", "category", "subcategory",
        "brand", "price", "stock_quantity", "product_status"
    ],
    "orders": [
        "order_id", "customer_id", "product_id", "order_timestamp",
        "quantity", "unit_price", "order_amount", "order_status",
        "payment_status"
    ],
    "payments": [
        "payment_id", "order_id", "customer_id", "payment_timestamp",
        "payment_method", "payment_status", "payment_amount",
        "transaction_reference"
    ],
    "events": [
        "event_id", "event_type", "customer_id", "event_timestamp",
        "source", "payload", "product_id", "order_id"
    ],
}


def read_json_objects(prefix):
    """Read every JSON data object under an S3 prefix."""
    paginator = s3.get_paginator("list_objects_v2")
    records = []

    for page in paginator.paginate(Bucket=BUCKET, Prefix=prefix):
        for obj in page.get("Contents", []):
            key = obj["Key"]

            # Ignore folder markers and non-JSON objects.
            if not key.lower().endswith(".json"):
                continue

            response = s3.get_object(Bucket=BUCKET, Key=key)
            body = response["Body"].read().decode("utf-8")

            try:
                parsed = json.loads(body)
            except json.JSONDecodeError as exc:
                raise ValueError(
                    f"Invalid JSON in s3://{BUCKET}/{key}: {exc}"
                ) from exc

            # Support both a single JSON object and a JSON array.
            if isinstance(parsed, dict):
                records.append((key, parsed))
            elif isinstance(parsed, list):
                for item in parsed:
                    if not isinstance(item, dict):
                        raise ValueError(
                            f"Non-object JSON record in s3://{BUCKET}/{key}"
                        )
                    records.append((key, item))
            else:
                raise ValueError(
                    f"Unexpected JSON structure in s3://{BUCKET}/{key}"
                )

    return records


def validation_column_from_check(check_name, dataset):
    """
    Map a failed validation check to a column using configured column names.

    If the check cannot be mapped unambiguously, return None.
    """
    normalized = re.sub(
        r"[^a-z0-9]+", " ",
        str(check_name).lower()
    ).strip()

    matches = []

    for column in COLUMN_NAMES[dataset]:
        normalized_column = column.lower().replace("_", " ")

        # Require the column name to appear as a complete phrase.
        pattern = rf"(?<![a-z0-9]){re.escape(normalized_column)}(?![a-z0-9])"

        if re.search(pattern, normalized):
            matches.append(column)

    if len(matches) == 1:
        return matches[0]

    # An ambiguous check cannot safely be counted.
    return None


def evaluate_validation_reports():
    records = read_json_objects(VALIDATION_PREFIX)
    reports_by_dataset = defaultdict(list)

    for key, report in records:
        dataset = str(report.get("dataset", "")).lower()

        if dataset in DATASETS:
            reports_by_dataset[dataset].append((key, report))

    failed_columns = defaultdict(set)
    issues = []

    for dataset in sorted(DATASETS):
        reports = reports_by_dataset.get(dataset, [])

        if len(reports) != 1:
            issues.append(
                f"{dataset}: expected exactly one validation report; "
                f"found {len(reports)}"
            )
            continue

        key, report = reports[0]

        checks = report.get("checks")
        if not isinstance(checks, list):
            issues.append(f"{dataset}: missing or invalid checks array")
            continue

        for check in checks:
            if not isinstance(check, dict):
                issues.append(f"{dataset}: malformed validation check")
                continue

            if str(check.get("status", "")).upper() != "FAIL":
                continue

            check_name = check.get("validation_name")
            if not check_name:
                issues.append(
                    f"{dataset}: failed check has no validation_name"
                )
                continue

            column = validation_column_from_check(
                check_name, dataset
            )

            if column is None:
                issues.append(
                    f"{dataset}: cannot map failed check "
                    f"'{check_name}' to exactly one column"
                )
            else:
                failed_columns[dataset].add(column)

        logger.info("Processed validation report: %s", key)

    return failed_columns, issues

def evaluate_quality_summaries():
    records = read_json_objects(QUALITY_PREFIX)

    quality_by_dataset = defaultdict(list)
    failed_columns = defaultdict(set)
    issues = []

    for key, record in records:
        dataset = str(record.get("dataset", "")).lower()

        if dataset in DATASETS:
            quality_by_dataset[dataset].append((key, record))

    for dataset in sorted(DATASETS):
        summaries = quality_by_dataset.get(dataset, [])

        if not summaries:
            issues.append(
                f"{dataset}: no quality summary record found"
            )
            continue

        # Multiple records can exist across Spark part files.
        # Require exactly one summary record per dataset to avoid
        # accidentally mixing records from different runs.
        if len(summaries) != 1:
            issues.append(
                f"{dataset}: expected one summary record, "
                f"found {len(summaries)}"
            )
            continue

        key, summary = summaries[0]

        status = str(summary.get("overall_status", "")).upper()
        if status not in {"PASS", "FAIL"}:
            issues.append(
                f"{dataset}: missing or invalid overall_status"
            )
            continue

        # The current combined summary has no failed-column details.
        if "failed_columns" not in summary:
            issues.append(
                f"{dataset}: {key} does not contain failed_columns"
            )
            continue

        columns = summary["failed_columns"]
        if not isinstance(columns, list):
            issues.append(
                f"{dataset}: failed_columns must be a list"
            )
            continue

        if any(not isinstance(c, str) or not c.strip() for c in columns):
            issues.append(
                f"{dataset}: invalid entry in failed_columns"
            )
            continue

        failed_columns[dataset].update(columns)

    return failed_columns, issues



def lambda_handler(event, context):
    try:
        validation_failures, validation_issues = (
            evaluate_validation_reports()
        )
        quality_failures, quality_issues = (
            evaluate_quality_summaries()
        )

        issues = validation_issues + quality_issues
        results = {}

        for dataset in sorted(DATASETS):
            # Count the union so the same column is not counted twice
            # when it fails both validation and quality checks.
            combined = (
                validation_failures[dataset]
                | quality_failures[dataset]
            )

            results[dataset] = {
                "failed_columns": sorted(combined),
                "failed_column_count": len(combined),
                "status": (
                    "PASS"
                    if len(combined) <= MAX_FAILED_COLUMNS
                    else "BLOCK"
                ),
            }

            if len(combined) > MAX_FAILED_COLUMNS:
                issues.append(
                    f"{dataset}: {len(combined)} distinct failed columns "
                    f"exceeds the limit of {MAX_FAILED_COLUMNS}"
                )

        decision = "PASS" if not issues else "BLOCK"

        result = {
            "decision": decision,
            "allow_refined": decision == "PASS",
            "max_failed_columns": MAX_FAILED_COLUMNS,
            "datasets": results,
            "issues": issues,
        }

        logger.info("Validation gate result: %s", json.dumps(result))
        return result

    except (ClientError, ValueError) as exc:
        logger.exception("Validation gate could not complete")
        return {
            "decision": "BLOCK",
            "allow_refined": False,
            "max_failed_columns": MAX_FAILED_COLUMNS,
            "datasets": {},
            "issues": [str(exc)],
        }
