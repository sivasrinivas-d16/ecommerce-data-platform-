from pyspark.sql import SparkSession
from pyspark.sql.functions import col, from_json
from pyspark.sql.types import (
    StructType,
    StructField,
    StringType
)

from pyspark.sql.functions import col, from_json, when,to_timestamp, window, count


spark = (
    SparkSession.builder
    .appName("EcommerceKafkaStreaming")
    .master("local[*]")
    .config("spark.driver.host", "127.0.0.1")
    .config("spark.driver.bindAddress", "127.0.0.1")
    .getOrCreate()
)

spark.sparkContext.setLogLevel("WARN")


# Define Kafka event schema
event_schema = StructType([
    StructField("event_id", StringType(), True),
    StructField("customer_id", StringType(), True),
    StructField("event_type", StringType(), True),
    StructField("event_timestamp", StringType(), True),
    StructField("source", StringType(), True)
])


# Read events from Kafka
events_df = (
    spark.readStream
    .format("kafka")
    .option("kafka.bootstrap.servers", "localhost:9092")
    .option("subscribe", "ecommerce-events")
    .option("startingOffsets", "earliest")
    .load()
)


# Convert Kafka binary value to string
raw_events = events_df.select(
    col("value").cast("string").alias("json_value")
)


# Parse JSON
parsed_events = (
    raw_events
    .select(
        from_json(
            col("json_value"),
            event_schema
        ).alias("event")
    )
    .select("event.*")
)

parsed_events = parsed_events.withColumn(
    "event_timestamp",
    to_timestamp("event_timestamp")
)

# Validate streaming events
validated_events = (
    parsed_events
    .withColumn(
        "validation_status",
        when(
            col("event_id").isNull(),
            "INVALID"
        )
        .when(
            col("customer_id").isNull(),
            "INVALID"
        )
        .when(
            ~col("event_type").isin(
                "PRODUCT_VIEWED",
                "ADD_TO_CART",
                "ORDER_CREATED",
                "PAYMENT_COMPLETED",
                "ORDER_SHIPPED",
                "ORDER_DELIVERED",
                "PAYMENT_FAILED"
            ),
            "INVALID"
        )
        .when(
            ~col("source").isin(
                "WEB",
                "MOBILE_APP",
                "API",
                "STORE"
            ),
            "INVALID"
        )
        .when(
            col("event_timestamp").isNull(),
            "INVALID"
        )
        .otherwise("VALID")
    )
)

watermarked_events = (
    validated_events
    .withWatermark("event_timestamp", "10 minutes")
)

deduplicated_events = (
    watermarked_events
    .dropDuplicates(["event_id"])
)

windowed_events = (
    deduplicated_events
    .groupBy(
        window(
            col("event_timestamp"),
            "1 minute"
        ),
        col("event_type")
    )
    .agg(
        count("*").alias("event_count")
    )
)

# 7. ADD THE NEW FUNCTION HERE
def write_idempotent_batch(batch_df, batch_id):

    output_path = r".\data\curated\streaming_events"

    if batch_df.isEmpty():
        print(f"Batch {batch_id}: No records to process")
        return

    try:
        existing_events = spark.read.parquet(output_path)

        existing_event_ids = existing_events.select(
            "event_id"
        ).dropDuplicates()

        new_events = batch_df.join(
            existing_event_ids,
            on="event_id",
            how="left_anti"
        )

    except Exception:
        new_events = batch_df

    new_count = new_events.count()

    if new_count == 0:
        print(f"Batch {batch_id}: No new events to write")
        return

    new_events.write \
        .mode("append") \
        .parquet(output_path)

    print(
        f"Batch {batch_id}: "
        f"{new_count} new events written"
    )


# Display validation result
query = (
    windowed_events.writeStream
    .format("console")
    .outputMode("update")
    .option(
        "checkpointLocation",
        r".\data\streaming\checkpoints\ecommerce_events_window1m"
    )
    .option("truncate", "false")
    .start()
)

query.awaitTermination()