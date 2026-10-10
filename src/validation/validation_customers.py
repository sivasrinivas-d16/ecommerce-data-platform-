
from datetime import datetime, timezone

from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import StringType, DateType, TimestampType


def validate(spark: SparkSession, bucket: str) -> dict:
    """Strict validation of the Raw Customers dataset."""

    dataset = "customers"
    input_path = f"s3://{bucket}/raw/customers/"

    required_columns = [
    "customer_id",
    "name",
    "email",
    "country",
    "state",
    "city",
    "signup_date",
    ]

    checks = []

    def add_check(name, invalid_count, details=None):
        invalid_count = int(invalid_count)
        checks.append({
            "validation_name": name,
            "invalid_records": invalid_count,
            "status": "PASS" if invalid_count == 0 else "FAIL",
            "details": details or "",
        })

    def invalid_count(df, condition):
        return df.filter(condition).count()

    # 1. Read Raw data. Read errors should fail the Glue job.
    df = spark.read.parquet(input_path)
    row_count = df.count()

    print(f"Loaded {row_count} customers from {input_path}")

    # 2. Required schema check.
    missing = [
        c for c in required_columns
        if c not in df.columns
    ]

    add_check(
        "Required Schema",
        len(missing),
        f"Missing columns: {missing}" if missing else "Schema present",
    )

    # Do not continue with column-based checks if schema is incomplete.
    if missing:
        return {
            "dataset": dataset,
            "row_count": row_count,
            "overall_status": "FAIL",
            "run_timestamp": datetime.now(timezone.utc).isoformat(),
            "checks": checks,
        }

    # 3. Unexpected columns: informational strictness check.
    unexpected = [
        c for c in df.columns
        if c not in required_columns
    ]

    add_check(
        "Unexpected Columns",
        len(unexpected),
        f"Unexpected columns: {unexpected}" if unexpected else "None",
    )

    # 4. Required values: nulls, empty strings, whitespace-only strings.
    for name in required_columns:
        value = F.col(name)

        blank_condition = (
            value.isNull()
            | (F.length(F.trim(value.cast("string"))) == 0)
        )

        add_check(
            f"Required Value: {name}",
            invalid_count(df, blank_condition),
        )

    # 5. Customer ID format.
    customer_id_invalid = (
        F.col("customer_id").isNull()
        | ~F.col("customer_id").rlike(r"^C[0-9]{5}$")
    )

    add_check(
        "Customer ID Format",
        invalid_count(df, customer_id_invalid),
        "Expected format: C followed by 5 digits",
    )

    # 6. Duplicate IDs.
    duplicate_id_count = (
        df.filter(F.col("customer_id").isNotNull())
        .groupBy("customer_id")
        .count()
        .filter(F.col("count") > 1)
        .count()
    )

    add_check("Duplicate Customer IDs", duplicate_id_count)

    # 7. Email: no surrounding whitespace and expected pattern.
    email = F.col("email")

    email_invalid = (
        email.isNull()
        | (F.length(F.trim(email)) == 0)
        | (email != F.trim(email))
        | ~email.rlike(
            r"^[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\.[A-Za-z]{2,}$"
        )
    )

    add_check("Email Format", invalid_count(df, email_invalid))

    # 8. Country must contain a nonblank value.
    country = F.col("country")

    country_invalid = (
        country.isNull()
        | (F.length(F.trim(country.cast("string"))) == 0)
    )

    add_check(
        "Country Validity",
        invalid_count(df, country_invalid),
        "Country must not be null or blank",
    )
    # 9. Signup date: safely parse to date and reject invalid/future dates.
    # Accepts date/timestamp types and common ISO-style string dates.
    signup_type = df.schema["signup_date"].dataType

    if isinstance(signup_type, (DateType, TimestampType)):
        parsed_signup = F.to_date(F.col("signup_date"))
    else:
        parsed_signup = F.coalesce(
            F.to_date(F.col("signup_date"), "yyyy-MM-dd"),
            F.to_date(F.col("signup_date"), "yyyy-MM-dd HH:mm:ss"),
            F.to_date(F.col("signup_date"), "yyyy/MM/dd"),
        )

    date_invalid = (
        F.col("signup_date").isNull()
        | parsed_signup.isNull()
        | (parsed_signup > F.current_date())
    )

    add_check("Signup Date Validity", invalid_count(df, date_invalid))

    # 10. Check the name field contains non-whitespace text.
    name_invalid = (
        F.col("name").isNull()
        | (F.length(F.trim(F.col("name"))) == 0)
    )

    add_check("Customer Name", invalid_count(df, name_invalid))

    # 11. Overall status: every check must pass.
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

    print(f"Overall {dataset} validation: {overall_status}")

    for check in checks:
        print(check)

    return report
