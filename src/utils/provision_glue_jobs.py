
import boto3
from botocore.exceptions import ClientError

# ============================================================
# 1. AWS CONFIGURATION
# ============================================================

AWS_REGION = "ap-southeast-2"

GLUE_ROLE_ARN = (
    "arn:aws:iam::256130491261:"
    "role/service-role/AWSGlueServiceRole-ecommerce"
)

S3_BUCKET = "ecommerce-data-platform-version1"

# ============================================================
# 2. SOURCE GENERATION JOB
# ============================================================

SOURCE_GENERATION_JOB = "ecommerce_generate_source_data"

SOURCE_GENERATION_SCRIPT = (
    f"s3://{S3_BUCKET}/scripts/generate_source_data.py"
)

# ============================================================
# 3. EXISTING CUSTOMERS JOBS
# ============================================================

CUSTOMERS_ETL_SCRIPT = (
    f"s3://{S3_BUCKET}/code/src/ingestion/glue_customers_etl.py"
)

CUSTOMERS_VALIDATION_QUALITY_SCRIPT = (
    f"s3://{S3_BUCKET}/code/src/validation_quality/"
    "glue_customers_validation_quality.py"
)

CUSTOMERS_TRANSFORMATION_SCRIPT = (
    f"s3://{S3_BUCKET}/code/src/transformation/"
    "transform_customers.py"
)

# ============================================================
# 4. EXISTING PRODUCTS JOB
# ============================================================

PRODUCTS_ETL_SCRIPT = (
    f"s3://{S3_BUCKET}/code/src/ingestion/ingest_products.py"
)

# ============================================================
# 5. RAW-LAYER JOB
# ============================================================

RAW_LAYER_JOB = "ecommerce_raw_layer"

RAW_LAYER_SCRIPT = (
    f"s3://{S3_BUCKET}/scripts/raw/raw_layer_main.py"
)

RAW_LAYER_MODULES = (
    f"s3://{S3_BUCKET}/scripts/raw/raw_modules.zip"
)


VALIDATION_MAIN_JOB = "ecommerce-validation-main"

VALIDATION_MAIN_SCRIPT = (
    f"s3://{S3_BUCKET}/scripts/validation_main.py"
)

VALIDATION_MODULES = ",".join([
    f"s3://{S3_BUCKET}/code/src/validation/validate_customers.py",
    f"s3://{S3_BUCKET}/code/src/validation/validate_products.py",
    f"s3://{S3_BUCKET}/code/src/validation/validate_orders.py",
    f"s3://{S3_BUCKET}/code/src/validation/validate_payments.py",
    f"s3://{S3_BUCKET}/code/src/validation/validate_events.py",
])

# ============================================================
# 6. GLUE CLIENT
# ============================================================

glue_client = boto3.client(
    "glue",
    region_name=AWS_REGION
)

# ============================================================
# 7. COMMON CONFIGURATION
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


def create_or_update_job(
    job_name,
    script_location,
    extra_arguments=None,
    worker_type="G.1X",
    number_of_workers=2,
    timeout=60
):
    job_config = {
        "Role": GLUE_ROLE_ARN,
        "Command": get_job_command(script_location),
        "GlueVersion": "5.1",
        "WorkerType": worker_type,
        "NumberOfWorkers": number_of_workers,
        "Timeout": timeout,
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
# 8. SOURCE DATA GENERATION
# ============================================================

def provision_source_generation_job():
    create_or_update_job(
        job_name=SOURCE_GENERATION_JOB,
        script_location=SOURCE_GENERATION_SCRIPT,
        worker_type="G.1X",
        number_of_workers=5,
        timeout=180
    )


# ============================================================
# 9. CUSTOMERS ETL
# ============================================================

def provision_customers_etl_job():
    create_or_update_job(
        job_name="ecommerce-customers-etl",
        script_location=CUSTOMERS_ETL_SCRIPT
    )


# ============================================================
# 10. CUSTOMERS VALIDATION + QUALITY
# ============================================================

def provision_customers_validation_quality_job():
    create_or_update_job(
        job_name="ecommerce-customers-validation-quality",
        script_location=CUSTOMERS_VALIDATION_QUALITY_SCRIPT
    )


# ============================================================
# 11. CUSTOMERS TRANSFORMATION
# ============================================================

def provision_customers_transformation_job():
    create_or_update_job(
        job_name="ecommerce-customers-transformation",
        script_location=CUSTOMERS_TRANSFORMATION_SCRIPT
    )


# ============================================================
# 12. PRODUCTS ETL
# ============================================================

def provision_products_etl_job():
    create_or_update_job(
        job_name="ecommerce-products-etl",
        script_location=PRODUCTS_ETL_SCRIPT
    )


# ============================================================
# 13. COMBINED RAW-LAYER JOB
# ============================================================

def provision_raw_layer_job():
    raw_layer_arguments = {
        "--extra-py-files": RAW_LAYER_MODULES,

        "--customers_input_path":
            f"s3://{S3_BUCKET}/data_s3/customers.csv/",
        "--customers_output_path":
            f"s3://{S3_BUCKET}/raw/customers/",

        "--products_input_path":
            f"s3://{S3_BUCKET}/data_s3/products.csv/",
        "--products_output_path":
            f"s3://{S3_BUCKET}/raw/products/",

        "--orders_input_path":
            f"s3://{S3_BUCKET}/data_s3/orders.csv/",
        "--orders_output_path":
            f"s3://{S3_BUCKET}/raw/orders/",

        "--payments_input_path":
            f"s3://{S3_BUCKET}/data_s3/payments.csv/",
        "--payments_output_path":
            f"s3://{S3_BUCKET}/raw/payments/",

        "--events_input_path":
            f"s3://{S3_BUCKET}/data_s3/events.csv/",
        "--events_output_path":
            f"s3://{S3_BUCKET}/raw/events/",

        "--write_mode": "overwrite"
    }

    create_or_update_job(
        job_name=RAW_LAYER_JOB,
        script_location=RAW_LAYER_SCRIPT,
        extra_arguments=raw_layer_arguments,
        worker_type="G.1X",
        number_of_workers=5,
        timeout=180
    )

# ============================================================
# VALIDATION MAIN GLUE JOB
# ============================================================

def provision_validation_main_job():

    validation_arguments = {
        "--S3_BUCKET": S3_BUCKET,
        "--extra-py-files": VALIDATION_MODULES,
    }

    create_or_update_job(
        job_name=VALIDATION_MAIN_JOB,
        script_location=VALIDATION_MAIN_SCRIPT,
        extra_arguments=validation_arguments,
        worker_type="G.1X",
        number_of_workers=5,
        timeout=180,
    )



# ============================================================
# MAIN
# ============================================================

def main():
    print("=" * 70)
    print("STARTING GLUE JOB PROVISIONING")
    print("=" * 70)

    print("\nProvisioning source-generation job...")
    provision_source_generation_job()

    print("\nProvisioning combined raw-layer job...")
    provision_raw_layer_job()

    print("\nProvisioning validation main job...")
    provision_validation_main_job()

    print("\n" + "=" * 70)
    print("GLUE JOB PROVISIONING COMPLETED")
    print("=" * 70)


if __name__ == "__main__":
    main()
