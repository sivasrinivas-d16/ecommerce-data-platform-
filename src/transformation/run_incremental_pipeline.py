from pyspark.sql import SparkSession
from pyspark.sql.utils import AnalysisException



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
# Paths
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


print("Incremental Order Pipeline Started")
print("-----------------------------------")
print(f"Input Path    : {incremental_input_path}")
print(f"Curated Path  : {curated_orders_path}")
print(f"Output Path   : {incremental_output_path}")

# =========================================================
# Read Incoming Incremental Batch
# =========================================================

incremental_df = (
    spark.read
    .parquet(incremental_input_path)
)

incoming_count = incremental_df.count()

print("\n--- Incoming Incremental Batch ---")
print(f"Incoming records: {incoming_count}")

# =========================================================
# Load Existing Curated Orders
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
# Identify New Orders
# =========================================================

new_orders_df = (
    incremental_df
    .join(
        existing_order_ids,
        on="order_id",
        how="left_anti"
    )
)

new_orders_count = new_orders_df.count()

duplicate_count = (
    incoming_count - new_orders_count
)


# =========================================================
# Display Incremental Detection Results
# =========================================================

print("\n--- Incremental Detection ---")
print(f"Incoming records : {incoming_count}")
print(f"Existing records : {duplicate_count}")
print(f"New records      : {new_orders_count}")

# =========================================================
# Transform New Orders
# =========================================================

from pyspark.sql.functions import (
    to_date,
    year,
    month,
    dayofmonth,
    col,
    when,
    round
)


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
        when(
            col("order_amount")
            == col("calculated_order_amount"),
            True
        ).otherwise(False)
    )

    # Business flags
    .withColumn(
        "is_completed_order",
        when(
            col("order_status") == "COMPLETED",
            True
        ).otherwise(False)
    )

    .withColumn(
        "is_paid_order",
        when(
            col("payment_status") == "PAID",
            True
        ).otherwise(False)
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
# Transformation Validation
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
    .filter(col("count") > 1)
    .count()
)


print("\n--- Transformation ---")
print(f"New records processed    : {processed_count}")
print(f"Invalid order amounts    : {invalid_amount_count}")
print(f"Duplicate order IDs      : {duplicate_new_orders}")

# =========================================================
# Write Processed Incremental Orders
# =========================================================

if processed_count > 0:

    (
        transformed_new_orders
        .write
        .mode("append")
        .partitionBy(
            "order_year",
            "order_month"
        )
        .parquet(incremental_output_path)
    )

    print("\n--- Incremental Write ---")
    print(
        f"Saved incremental records: "
        f"{processed_count}"
    )

else:

    print("\n--- Incremental Write ---")
    print("No new records to write.")

# =========================================================
# Verify Incremental Output
# =========================================================

written_df = (
    spark.read
    .parquet(incremental_output_path)
)

written_count = written_df.count()

print("\n--- Output Verification ---")
print(f"Total records in output: {written_count}")

print("\n--- Output Sample ---")

(
    written_df
    .select(
        "order_id",
        "customer_id",
        "product_id",
        "quantity",
        "order_amount",
        "order_status",
        "payment_status",
        "order_date",
        "order_year",
        "order_month"
    )
    .orderBy("order_id")
    .show(10, truncate=False)
)
# =========================================================
# Load Existing Processed Order IDs
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
# Load Previously Processed Incremental IDs
# =========================================================



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

    print("\nPreviously processed incremental output found.")

except AnalysisException:

    processed_incremental_ids = (
        spark.createDataFrame(
            [],
            existing_order_ids.schema
        )
    )

    print("\nNo previous incremental output found.")


# =========================================================
# Combine Existing + Previously Processed IDs
# =========================================================

all_processed_order_ids = (
    existing_order_ids
    .union(processed_incremental_ids)
    .distinct()
)


# =========================================================
# Identify Truly New Orders
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


# =========================================================
# Display Detection Results
# =========================================================

print("\n--- Incremental Detection ---")
print(f"Incoming records : {incoming_count}")
print(f"Existing records : {duplicate_count}")
print(f"New records      : {new_orders_count}")
