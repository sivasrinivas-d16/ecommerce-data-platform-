from datetime import datetime

from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col,
    to_date,
    year,
    month,
    dayofmonth,
    round,
    when
)


# =========================================================
# Spark Session
# =========================================================

spark = (
    SparkSession.builder
    .appName("IncrementalOrderPipeline")
    .master("local[*]")
    .config("spark.driver.host", "127.0.0.1")
    .config("spark.driver.bindAddress", "127.0.0.1")
    .getOrCreate()
)

spark.sparkContext.setLogLevel("WARN")


# =========================================================
# Pipeline Configuration
# =========================================================

incremental_input_path = (
    r".\ecommerce-data-platform\data\raw\incremental\orders_batch_002"
)

curated_orders_path = (
    r".\ecommerce-data-platform\data\curated\orders"
)

incremental_output_path = (
    r".\ecommerce-data-platform\data\curated\incremental_orders"
)

audit_output_path = (
    r".\ecommerce-data-platform\data\curated\pipeline_audit"
)

batch_id = "BATCH_002"

pipeline_name = "IncrementalOrderPipeline"

run_timestamp = datetime.now()


print("Incremental Order Pipeline Started")
print("-----------------------------------")
print(f"Input Path    : {incremental_input_path}")
print(f"Curated Path  : {curated_orders_path}")
print(f"Output Path   : {incremental_output_path}")
print(f"Audit Path    : {audit_output_path}")


# =========================================================
# Step 1 — Read Incoming Incremental Batch
# =========================================================

incremental_df = (
    spark.read
    .parquet(incremental_input_path)
)

incoming_count = incremental_df.count()

print("\n--- Incoming Incremental Batch ---")
print(f"Incoming records: {incoming_count}")


# =========================================================
# Step 2 — Load Existing Curated Orders
# =========================================================

existing_orders_df = (
    spark.read
    .parquet(curated_orders_path)
)

existing_order_ids = (
    existing_orders_df
    .select("order_id")
    .distinct()
)


# =========================================================
# Step 3 — Load Previously Processed Incremental IDs
# =========================================================

from pyspark.sql.utils import AnalysisException


try:

    processed_incremental_df = (
        spark.read
        .parquet(incremental_output_path)
    )

    processed_incremental_ids = (
        processed_incremental_df
        .select("order_id")
        .distinct()
    )

    print(
        "\nPreviously processed "
        "incremental output found."
    )

except AnalysisException:

    processed_incremental_ids = (
        spark.createDataFrame(
            [],
            existing_order_ids.schema
        )
    )

    print(
        "\nNo previous incremental "
        "output found."
    )


# =========================================================
# Step 4 — Combine All Processed IDs
# =========================================================

all_processed_order_ids = (
    existing_order_ids
    .union(processed_incremental_ids)
    .distinct()
)


# =========================================================
# Step 5 — Identify Truly New Orders
# =========================================================

new_orders_df = (
    incremental_df
    .join(
        all_processed_order_ids,
        on="order_id",
        how="left_anti"
    )
)

new_orders_count = new_orders_df.count()

duplicate_count = (
    incoming_count - new_orders_count
)


print("\n--- Incremental Detection ---")
print(f"Incoming records : {incoming_count}")
print(f"Existing records : {duplicate_count}")
print(f"New records      : {new_orders_count}")


# =========================================================
# Step 6 — Transform New Orders
# =========================================================

transformed_new_orders = (
    new_orders_df

    # Date attributes
    .withColumn(
        "order_date",
        to_date("order_timestamp")
    )

    .withColumn(
        "order_year",
        year("order_timestamp")
    )

    .withColumn(
        "order_month",
        month("order_timestamp")
    )

    .withColumn(
        "order_day",
        dayofmonth("order_timestamp")
    )

    # Recalculate order amount
    .withColumn(
        "calculated_order_amount",
        round(
            col("quantity") * col("unit_price"),
            2
        )
    )

    # Validate source order amount
    .withColumn(
        "order_amount_valid",
        col("order_amount")
        == col("calculated_order_amount")
    )

    # Completed order flag
    .withColumn(
        "is_completed_order",
        col("order_status") == "COMPLETED"
    )

    # Paid order flag
    .withColumn(
        "is_paid_order",
        col("payment_status") == "PAID"
    )

    # Net revenue
    .withColumn(
        "net_order_amount",
        when(
            col("order_status") == "COMPLETED",
            col("order_amount")
        ).otherwise(0)
    )
)


# =========================================================
# Step 7 — Transformation Validation
# =========================================================

processed_count = transformed_new_orders.count()

invalid_amount_count = (
    transformed_new_orders
    .filter(
        ~col("order_amount_valid")
    )
    .count()
)

duplicate_new_orders = (
    transformed_new_orders
    .groupBy("order_id")
    .count()
    .filter(
        col("count") > 1
    )
    .count()
)


print("\n--- Transformation ---")
print(
    f"New records processed    : "
    f"{processed_count}"
)

print(
    f"Invalid order amounts    : "
    f"{invalid_amount_count}"
)

print(
    f"Duplicate order IDs      : "
    f"{duplicate_new_orders}"
)


# =========================================================
# Step 8 — Pipeline Status
# =========================================================

pipeline_status = "SUCCESS"

if invalid_amount_count > 0:
    pipeline_status = "FAILED"

if duplicate_new_orders > 0:
    pipeline_status = "FAILED"


# =========================================================
# Step 9 — Write Processed Incremental Orders
# =========================================================

if (
    processed_count > 0
    and pipeline_status == "SUCCESS"
):

    (
        transformed_new_orders
        .write
        .mode("append")
        .partitionBy(
            "order_year",
            "order_month"
        )
        .parquet(
            incremental_output_path
        )
    )

    print("\n--- Incremental Write ---")
    print(
        f"Saved incremental records: "
        f"{processed_count}"
    )

else:

    print("\n--- Incremental Write ---")
    print(
        "No new records to write."
    )


# =========================================================
# Step 10 — Create Real Audit Record
# =========================================================

audit_data = [
    (
        batch_id,
        pipeline_name,
        run_timestamp,
        incoming_count,
        duplicate_count,
        new_orders_count,
        processed_count,
        invalid_amount_count,
        pipeline_status,
        incremental_output_path
    )
]

audit_columns = [
    "batch_id",
    "pipeline_name",
    "run_timestamp",
    "input_records",
    "duplicate_records",
    "new_records",
    "processed_records",
    "invalid_records",
    "pipeline_status",
    "output_path"
]


audit_df = spark.createDataFrame(
    audit_data,
    audit_columns
)


print("\n--- Pipeline Audit ---")

audit_df.show(
    truncate=False
)


# =========================================================
# Step 11 — Persist Audit Record
# =========================================================

(
    audit_df
    .write
    .mode("append")
    .parquet(
        audit_output_path
    )
)


print(
    "\nAudit record saved successfully."
)


# =========================================================
# Step 12 — Verify Audit History
# =========================================================

saved_audit_df = (
    spark.read
    .parquet(audit_output_path)
)

print("\n--- Audit History ---")

(
    saved_audit_df
    .orderBy(
        col("run_timestamp").desc()
    )
    .show(
        truncate=False
    )
)

print(
    f"Total audit records: "
    f"{saved_audit_df.count()}"
)


# =========================================================
# Step 13 — Output Verification
# =========================================================

if pipeline_status == "SUCCESS":

    try:

        written_df = (
            spark.read
            .parquet(incremental_output_path)
        )

        written_count = (
            written_df.count()
        )

        print("\n--- Output Verification ---")

        print(
            f"Total records in output: "
            f"{written_count}"
        )

    except AnalysisException:

        print(
            "\n--- Output Verification ---"
        )

        print(
            "No incremental output "
            "currently exists."
        )


# =========================================================
# Stop Spark
# =========================================================

spark.stop()