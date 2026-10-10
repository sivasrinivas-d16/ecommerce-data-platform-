
from pyspark.sql.types import (
    StructType,
    StructField,
    StringType
)
from pyspark.sql.functions import col, to_date


def process_customers(
    spark,
    input_path,
    output_path,
    write_mode
):
    """
    Ingest customer CSV data and write it to the raw layer.

    Parameters:
        spark: Spark session managed by AWS Glue
        input_path: Source CSV S3 path
        output_path: Raw output S3 path
        write_mode: Output write mode
    """

    # -----------------------------------------------------
    # 1. Customer Schema
    # -----------------------------------------------------

    customer_schema = StructType([
        StructField("customer_id", StringType(), True),
        StructField("name", StringType(), True),
        StructField("email", StringType(), True),
        StructField("city", StringType(), True),
        StructField("state", StringType(), True),
        StructField("country", StringType(), True),
        StructField("signup_date", StringType(), True)
    ])

    # -----------------------------------------------------
    # 2. Read Customer Data
    # -----------------------------------------------------

    customers_df = (
        spark.read
        .option("header", True)
        .schema(customer_schema)
        .csv(input_path)
    )

    print("Customers loaded successfully")
    print("Input path:", input_path)
    print("Row count:", customers_df.count())

    customers_df.printSchema()
    customers_df.show(5, truncate=False)

    # -----------------------------------------------------
    # 3. Convert signup_date
    # -----------------------------------------------------

    customers_df = customers_df.withColumn(
        "signup_date",
        to_date(col("signup_date"), "dd-MM-yyyy")
    )

    print("Customer signup_date conversion completed")
    customers_df.printSchema()
    customers_df.show(5, truncate=False)

    # -----------------------------------------------------
    # 4. Write to Raw Layer
    # -----------------------------------------------------

    (
        customers_df.write
        .mode(write_mode)
        .format("parquet")
        .save(output_path)
    )

    print("Customers written successfully to raw layer")
    print("Output path:", output_path)

    # -----------------------------------------------------
    # 5. Verify Raw Output
    # -----------------------------------------------------

    raw_customers_df = spark.read.parquet(output_path)

    print(
        "Raw customer row count:",
        raw_customers_df.count()
    )

    raw_customers_df.printSchema()
    raw_customers_df.show(5, truncate=False)

    print("Customers raw ingestion completed")
