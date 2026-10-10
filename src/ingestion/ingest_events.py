
from pyspark.sql.types import (
    StructType,
    StructField,
    StringType
)
from pyspark.sql.functions import (
    col,
    trim,
    to_timestamp,
    when,
    sum as spark_sum
)


def process_events(
    spark,
    input_path,
    output_path,
    write_mode
):
    """
    Ingest event CSV data and write it to the raw Parquet layer.

    Preserves all source rows and reports missing or invalid values.
    """

    # 1. Define the source CSV schema.
    # Read the timestamp as a string so conversion is explicit.
    event_schema = StructType([
        StructField("event_id", StringType(), True),
        StructField("event_type", StringType(), True),
        StructField("customer_id", StringType(), True),
        StructField("order_id", StringType(), True),
        StructField("product_id", StringType(), True),
        StructField("event_timestamp", StringType(), True),
        StructField("source", StringType(), True),
        StructField("payload", StringType(), True)
    ])

    # 2. Read source data.
    events_df = (
        spark.read
        .option("header", True)
        .option("mode", "PERMISSIVE")
        .schema(event_schema)
        .csv(input_path)
    )

    source_count = events_df.count()

    print(f"Events source row count: {source_count}")
    print(f"Input path: {input_path}")

    events_df.printSchema()
    events_df.show(5, truncate=False)

    # 3. Normalize whitespace in string fields.
    for column_name in events_df.columns:
        events_df = events_df.withColumn(
            column_name,
            trim(col(column_name))
        )

    # 4. Convert the timestamp explicitly.
    # The generator writes timestamps as yyyy-MM-dd HH:mm:ss.
 
# 4. Convert ISO 8601 timestamps explicitly.
    events_df = events_df.withColumn(
        "event_timestamp",
        to_timestamp(
            col("event_timestamp"),
            "yyyy-MM-dd'T'HH:mm:ss.SSSX"
        )
    )

    # 5. Report missing IDs and invalid timestamps.
    quality_summary = events_df.agg(
        spark_sum(
            when(
                col("event_id").isNull()
                | (col("event_id") == ""),
                1
            ).otherwise(0)
        ).alias("missing_event_id"),
        spark_sum(
            when(col("event_timestamp").isNull(), 1)
            .otherwise(0)
        ).alias("missing_or_invalid_timestamp")
    ).first()

    print("Missing event IDs:", quality_summary["missing_event_id"])
    print(
        "Missing or invalid event timestamps:",
        quality_summary["missing_or_invalid_timestamp"]
    )

    # 6. Write the raw Parquet output.
    (
        events_df.write
        .mode(write_mode)
        .format("parquet")
        .save(output_path)
    )

    print(f"Events written to: {output_path}")

    # 7. Verify the raw output.
    raw_events_df = spark.read.parquet(output_path)
    raw_count = raw_events_df.count()

    print(f"Source rows: {source_count}")
    print(f"Raw rows:    {raw_count}")

    if source_count != raw_count:
        raise RuntimeError(
            "Event row-count mismatch: "
            f"source={source_count}, raw={raw_count}"
        )

    print("Raw event schema:")
    raw_events_df.printSchema()

    print("Raw event sample:")
    raw_events_df.show(5, truncate=False)

    print("Events raw ingestion completed successfully.")
