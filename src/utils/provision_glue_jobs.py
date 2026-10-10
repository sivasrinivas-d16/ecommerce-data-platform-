import boto3


# ============================================================
# 1. AWS CONFIGURATION
# ============================================================

AWS_REGION = "ap-southeast-2"

GLUE_ROLE_ARN = (
   "arn:aws:iam::256130491261:role/service-role/AWSGlueServiceRole-ecommerce"
)

S3_BUCKET = "ecommerce-data-platform-version1"


# ============================================================
# 2. CUSTOMERS SCRIPTS
# ============================================================

CUSTOMERS_ETL_SCRIPT = (
    f"s3://{S3_BUCKET}/"
    "code/src/ingestion/glue_customers_etl.py"
)

CUSTOMERS_VALIDATION_QUALITY_SCRIPT = (
    f"s3://{S3_BUCKET}/"
    "code/src/validation_quality/"
    "glue_customers_validation_quality.py"
)

CUSTOMERS_TRANSFORMATION_SCRIPT = (
    f"s3://{S3_BUCKET}/"
    "code/src/transformation/"
    "transform_customers.py"
)


# ============================================================
# 3. PRODUCTS SCRIPT
# ============================================================

PRODUCTS_ETL_SCRIPT = (
    f"s3://{S3_BUCKET}/"
    "code/src/ingestion/ingest_products.py"
)


# ============================================================
# 4. COMBINED RAW-LAYER SCRIPT AND MODULES
# ============================================================

RAW_LAYER_JOB = "ecommerce_raw_layer"

RAW_LAYER_SCRIPT = (
    f"s3://{S3_BUCKET}/"
    "scripts/raw/raw_layer_main.py"
)

RAW_LAYER_MODULES = (
    f"s3://{S3_BUCKET}/"
    "scripts/raw/raw_modules.zip"
)


# ============================================================
# 5. GLUE CLIENT
# ============================================================

glue_client = boto3.client(
    "glue",
    region_name=AWS_REGION
)


# ============================================================
# 6. COMMON CONFIGURATION
# ============================================================

def get_job_command(script_location):
    return {
        "Name": "glueetl",
        "ScriptLocation": script_location,
        "PythonVersion": "3"
    }


def get_job_arguments():
    return {
        "--enable-glue-datacatalog": "true",
        "--enable-metrics": "true",
        "--enable-continuous-cloudwatch-log": "true",
        "--enable-job-insights": "true"
    }


def create_or_update_job(job_name, script_location, extra_arguments=None):
    job_config = {
        "Role": GLUE_ROLE_ARN,
        "Command": get_job_command(script_location),
        "GlueVersion": "5.1",
        "WorkerType": "G.1X",
        "NumberOfWorkers": 2,
        "Timeout": 15,
        "MaxRetries": 0,
        "ExecutionProperty": {
            "MaxConcurrentRuns": 1
        },
        "DefaultArguments": get_job_arguments()
    }

    if extra_arguments:
        job_config["DefaultArguments"].update(extra_arguments)

    try:
        glue_client.get_job(JobName=job_name)

        glue_client.update_job(
            JobName=job_name,
            JobUpdate=job_config
        )

        print(f"UPDATED Glue job: {job_name}")

    except glue_client.exceptions.EntityNotFoundException:
        glue_client.create_job(
            Name=job_name,
            **job_config
        )

        print(f"CREATED Glue job: {job_name}")


# ============================================================
# 7. CUSTOMERS ETL
# ============================================================

def provision_customers_etl_job():
    create_or_update_job(
        job_name="ecommerce-customers-etl",
        script_location=CUSTOMERS_ETL_SCRIPT
    )


# ============================================================
# 8. CUSTOMERS VALIDATION + QUALITY
# ============================================================

def provision_customers_validation_quality_job():
    create_or_update_job(
        job_name="ecommerce-customers-validation-quality",
        script_location=CUSTOMERS_VALIDATION_QUALITY_SCRIPT
    )


# ============================================================
# 9. CUSTOMERS TRANSFORMATION
# ============================================================

def provision_customers_transformation_job():
    create_or_update_job(
        job_name="ecommerce-customers-transformation",
        script_location=CUSTOMERS_TRANSFORMATION_SCRIPT
    )


# ============================================================
# 10. PRODUCTS ETL
# ============================================================

def provision_products_etl_job():
    create_or_update_job(
        job_name="ecommerce-products-etl",
        script_location=PRODUCTS_ETL_SCRIPT
    )


# ============================================================
# 11. COMBINED RAW-LAYER JOB
# ============================================================

def provision_raw_layer_job():

    raw_layer_arguments = {
        "--extra-py-files": RAW_LAYER_MODULES,

        # Replace these example source keys with the actual
        # S3 source locations verified in your bucket.
        "--customers_input_path":
            f"s3://{S3_BUCKET}/data_s3/customers.csv",
        "--customers_output_path":
            f"s3://{S3_BUCKET}/raw/customers/",

        "--products_input_path":
            f"s3://{S3_BUCKET}/data_s3/products.csv",
        "--products_output_path":
            f"s3://{S3_BUCKET}/raw/products_parquet/",

        "--orders_input_path":
            f"s3://{S3_BUCKET}/data_s3/orders.csv",
        "--orders_output_path":
            f"s3://{S3_BUCKET}/raw/orders/",

        "--payments_input_path":
            f"s3://{S3_BUCKET}/data_s3/payments.csv",
        "--payments_output_path":
            f"s3://{S3_BUCKET}/raw/payments/",

        "--events_input_path":
            f"s3://{S3_BUCKET}/data_s3/events.csv",
        "--events_output_path":
            f"s3://{S3_BUCKET}/raw/events/",

        "--write_mode": "overwrite"
    }

    create_or_update_job(
        job_name=RAW_LAYER_JOB,
        script_location=RAW_LAYER_SCRIPT,
        extra_arguments=raw_layer_arguments
    )


# ============================================================
# 12. MAIN
# ============================================================

def main():

    print("=" * 70)
    print("STARTING GLUE JOB PROVISIONING")
    print("=" * 70)

    print("\nProvisioning Customers jobs...")
    provision_customers_etl_job()
    provision_customers_validation_quality_job()
    provision_customers_transformation_job()

    print("\nProvisioning Products jobs...")
    provision_products_etl_job()

    print("\nProvisioning combined raw-layer job...")
    provision_raw_layer_job()

    print("\n" + "=" * 70)
    print("GLUE JOB PROVISIONING COMPLETED")
    print("=" * 70)


if __name__ == "__main__":
    main()
