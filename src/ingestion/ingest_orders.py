
from pyspark.sql.types import StructType, StructField, StringType
from pyspark.sql.functions import (
    col,
    trim,
    to_timestamp,
    when,
    sum as spark_sum
)
from pyspark.sql.types import IntegerType, DecimalType


def process_orders(
    spark,
    input_path,
    output_path,
    write_mode
):
    """
    Read Orders CSV, validate source data, convert types,
    and write the dataset to the S3 raw layer.
    """

    # 1. Read source CSV fields as strings for explicit conversion.
    source_schema = StructType([
        StructField("order_id", StringType(), True),
        StructField("customer_id", StringType(), True),
        StructField("product_id", StringType(), True),
        StructField("quantity", StringType(), True),
        StructField("unit_price", StringType(), True),
        StructField("order_amount", StringType(), True),
        StructField("order_status", StringType(), True),
        StructField("payment_status", StringType(), True),
        StructField("order_timestamp", StringType(), True)
    ])

    orders_df = (
        spark.read
        .option("header", True)
        .option("mode", "PERMISSIVE")
        .schema(source_schema)
        .csv(input_path)
    )

    source_count = orders_df.count()
    print(f"Orders source row count: {source_count}")
    orders_df.printSchema()

    # 2. Trim whitespace in source fields.
    for column_name in orders_df.columns:
        orders_df = orders_df.withColumn(
            column_name,
            trim(col(column_name))
        )

    # 3. Convert numeric and timestamp fields explicitly.
    orders_df = (
        orders_df
        .withColumn("quantity", col("quantity").cast(IntegerType()))
        .withColumn("unit_price", col("unit_price").cast(DecimalType(12, 2)))
        .withColumn("order_amount", col("order_amount").cast(DecimalType(14, 2)))
        .withColumn(
            "order_timestamp",
            to_timestamp(
                col("order_timestamp"),
                "yyyy-MM-dd HH:mm:ss"
            )
        )
    )

    # 4. Report missing required fields.
    required_columns = [
        "order_id",
        "customer_id",
        "product_id",
        "quantity",
        "unit_price",
        "order_amount",
        "order_status",
        "payment_status",
        "order_timestamp"
    ]

    null_summary = orders_df.agg(*[
        spark_sum(
            when(col(c).isNull(), 1).otherwise(0)
        ).alias(c)
        for c in required_columns
    ]).first()

    print("Null or failed-conversion counts:")
    for c in required_columns:
        print(f"{c}: {null_summary[c]}")

    # 5. Report duplicate order IDs.
    duplicate_order_ids = (
        orders_df.groupBy("order_id")
        .count()
        .filter(
            col("order_id").isNotNull()
            & (col("count") > 1)
        )
        .count()
    )

    print(f"Duplicate order IDs: {duplicate_order_ids}")

    # 6. Write all rows to the raw layer.
    (
        orders_df.write
        .mode(write_mode)
        .format("parquet")
        .save(output_path)
    )

    print(f"Orders written to: {output_path}")

    # 7. Verify raw output.
    raw_orders_df = spark.read.parquet(output_path)
    raw_count = raw_orders_df.count()

    print(f"Source rows: {source_count}")
    print(f"Raw rows:    {raw_count}")

    if raw_count != source_count:
        raise RuntimeError(
            f"Orders row-count mismatch: "
            f"source={source_count}, raw={raw_count}"
        )

    raw_orders_df.printSchema()
    raw_orders_df.show(5, truncate=False)

    print("Orders raw ingestion completed successfully.")
