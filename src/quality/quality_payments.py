from pyspark.sql import SparkSession
from pyspark.sql.functions import col

from pyspark.sql.types import StructType, StructField, StringType, DoubleType



spark = (
    SparkSession.builder
    .appName("ECommercePaymentQuality")
    .master("local[2]")
    .config("spark.driver.host", "127.0.0.1")
    .config("spark.driver.bindAddress", "127.0.0.1")
    .config("spark.hadoop.fs.permissions.umask-mode", "000")
    .getOrCreate()
)

payments_path = r".\ecommerce-data-platform\data\raw\payments"

payments_df = spark.read.parquet(payments_path)

print("Payments loaded for quality assessment")
print("Row count:", payments_df.count())

payments_df.printSchema()

print("\n--- Completeness Quality ---")

quality_columns = [
    "payment_id",
    "order_id",
    "customer_id",
    "payment_method",
    "payment_status",
    "payment_amount",
    "transaction_reference",
    "payment_timestamp"
]

total_records = payments_df.count()

completeness_scores = []

for column_name in quality_columns:

    non_null_count = (
        payments_df
        .filter(col(column_name).isNotNull())
        .count()
    )

    completeness = (non_null_count / total_records) * 100

    completeness_scores.append(completeness)

    print(
        f"{column_name}: "
        f"{completeness:.2f}% completeness"
    )

print("\n--- Payment ID Uniqueness Quality ---")

distinct_payment_ids = (
    payments_df
    .select("payment_id")
    .distinct()
    .count()
)

payment_id_uniqueness = (
    distinct_payment_ids / total_records
) * 100

print(
    f"payment_id uniqueness: "
    f"{payment_id_uniqueness:.2f}%"
)

print("\n--- Transaction Reference Uniqueness Quality ---")

distinct_transaction_refs = (
    payments_df
    .select("transaction_reference")
    .distinct()
    .count()
)

transaction_reference_uniqueness = (
    distinct_transaction_refs / total_records
) * 100

print(
    f"transaction_reference uniqueness: "
    f"{transaction_reference_uniqueness:.2f}%"
)

print("\n--- Payment Amount Validity Quality ---")

valid_payment_amounts = (
    payments_df
    .filter(col("payment_amount") > 0)
    .count()
)

payment_amount_validity = (
    valid_payment_amounts / total_records
) * 100

print(
    f"payment_amount validity: "
    f"{payment_amount_validity:.2f}%"
)

print("\n--- Payment Status Validity Quality ---")

valid_payment_statuses = [
    "PAID",
    "PENDING",
    "FAILED",
    "REFUNDED"
]

valid_status_count = (
    payments_df
    .filter(col("payment_status").isin(valid_payment_statuses))
    .count()
)

payment_status_validity = (
    valid_status_count / total_records
) * 100

print(
    f"payment_status validity: "
    f"{payment_status_validity:.2f}%"
)

print("\n--- Payment Method Validity Quality ---")

valid_payment_methods = [
    "UPI",
    "CREDIT_CARD",
    "DEBIT_CARD",
    "NET_BANKING",
    "WALLET"
]

valid_method_count = (
    payments_df
    .filter(col("payment_method").isin(valid_payment_methods))
    .count()
)

payment_method_validity = (
    valid_method_count / total_records
) * 100

print(
    f"payment_method validity: "
    f"{payment_method_validity:.2f}%"
)

quality_scores = []

quality_scores.append(
    sum(completeness_scores) / len(completeness_scores)
)

quality_scores.append(payment_id_uniqueness)

quality_scores.append(transaction_reference_uniqueness)

quality_scores.append(payment_amount_validity)

quality_scores.append(payment_status_validity)

quality_scores.append(payment_method_validity)

overall_quality_score = (
    sum(quality_scores) / len(quality_scores)
)

print(
    f"Overall Payment Quality Score: "
    f"{overall_quality_score:.2f}%"
)

quality_report = [
    ("completeness", quality_scores[0]),
    ("payment_id_uniqueness", quality_scores[1]),
    ("transaction_reference_uniqueness", quality_scores[2]),
    ("payment_amount_validity", quality_scores[3]),
    ("payment_status_validity", quality_scores[4]),
    ("payment_method_validity", quality_scores[5]),
    ("overall_quality_score", overall_quality_score)
]

quality_schema = StructType([
    StructField("metric_name", StringType(), False),
    StructField("score", DoubleType(), False)
])

quality_report_df = spark.createDataFrame(
    quality_report,
    schema=quality_schema
)

quality_report_path = (
    r".\ecommerce-data-platform\data\processed\quality\payments"
)

(
    quality_report_df.write
    .mode("overwrite")
    .parquet(quality_report_path)
)

print("\nPayment quality report persisted successfully.")

print(
    "Persisted quality metrics:",
    quality_report_df.count()
)

quality_report_df.show(truncate=False)

spark.stop()