from pyspark.sql import SparkSession
from pyspark.sql.types import (
    StructType,
    StructField,
    StringType,
    TimestampType
)

spark = (
    SparkSession.builder
    .appName("ECommerceEventIngestion")
    .master("local[2]")
    .config("spark.driver.host", "127.0.0.1")
    .config("spark.driver.bindAddress", "127.0.0.1")
    .config("spark.hadoop.fs.permissions.umask-mode", "000")
    .getOrCreate()
)

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

events_path = r".\ecommerce-data-platform\data\events.csv"

events_df = (
    spark.read
    .option("header", True)
    .schema(event_schema)
    .csv(events_path)
)

print("Events loaded successfully")
print("Row count:", events_df.count())

print("\nEvent Schema:")
events_df.printSchema()

print("\nEvent Sample:")
events_df.show(5, truncate=False)

raw_events_path = r".\ecommerce-data-platform\data\raw\events"

(
    events_df.write
    .mode("overwrite")
    .parquet(raw_events_path)
)

print("\nEvents written successfully to Raw layer.")

raw_events_df = spark.read.parquet(raw_events_path)

print("\nRaw event row count:", raw_events_df.count())

print("\nRaw Event Schema:")
raw_events_df.printSchema()

print("\nRaw Event Sample:")
raw_events_df.show(5, truncate=False)

spark.stop()

