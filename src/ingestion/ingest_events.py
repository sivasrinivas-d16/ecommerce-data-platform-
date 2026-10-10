
from pyspark.sql.types import (
    StructType,
    StructField,
    StringType,
    TimestampType
)


def process_events(
    spark,
    input_path,
    output_path,
    write_mode
):
    """
    Ingest event CSV data and write it to the raw layer.

    Parameters:
        spark: Spark session managed by AWS Glue
        input_path: Source CSV S3 path
        output_path: Raw output S3 path
        write_mode: Output write mode
    """

    # -----------------------------------------------------
    # 1. Event Schema
    # -----------------------------------------------------

    event_schema = StructType([
        StructField("event_id", StringType(), True),
        StructField("event_type", StringType(), True),
        StructField("customer_id", StringType(), True),
        StructField("order_id", StringType(), True),
        StructField("product_id", StringType(), True),
        StructField("event_timestamp", TimestampType(), True),
        StructField("source", StringType(), True),
        StructField("payload", StringType(), True)
    ])

    # -----------------------------------------------------
    # 2. Read Event Data
    # -----------------------------------------------------

    events_df = (
        spark.read
        .option("header", True)
        .schema(event_schema)
        .csv(input_path)
    )

    print("Events loaded successfully")
    print("Input path:", input_path)
    print("Row count:", events_df.count())

    print("Event Schema:")
    events_df.printSchema()

    print("Event Sample:")
    events_df.show(5, truncate=False)

    # -----------------------------------------------------
    # 3. Write to Raw Layer
    # -----------------------------------------------------

    (
        events_df.write
        .mode(write_mode)
        .format("parquet")
        .save(output_path)
    )

    print("Events written successfully to raw layer")
    print("Output path:", output_path)

    # -----------------------------------------------------
    # 4. Verify Raw Output
    # -----------------------------------------------------

    raw_events_df = spark.read.parquet(output_path)

    print(
        "Raw event row count:",
        raw_events_df.count()
    )

    print("Raw Event Schema:")
    raw_events_df.printSchema()

    print("Raw Event Sample:")
    raw_events_df.show(5, truncate=False)

    print("Events raw ingestion completed")
