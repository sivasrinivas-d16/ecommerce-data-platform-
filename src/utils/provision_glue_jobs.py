import boto3


# ============================================================
# AWS CONFIGURATION
# ============================================================

AWS_REGION = "ap-southeast-2"

GLUE_ROLE_ARN = (
    "arn:aws:iam::256130491261:"
    "role/AWSGlueServiceRole-ecommerce"
)

S3_BUCKET = "ecommerce-data-platform-version1"


# ============================================================
# CUSTOMERS SCRIPTS
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
# PRODUCTS SCRIPTS
# ============================================================

PRODUCTS_ETL_SCRIPT = (
    f"s3://{S3_BUCKET}/"
    "code/src/ingestion/ingest_products.py"
)


# ============================================================
# GLUE CLIENT
# ============================================================

glue_client = boto3.client(
    "glue",
    region_name=AWS_REGION
)


# ============================================================
# COMMON GLUE JOB CONFIGURATION
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
    script_location
):
    job_config = {
        "Role": GLUE_ROLE_ARN,

        "Command": get_job_command(
            script_location
        ),

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

    try:

        glue_client.get_job(
            JobName=job_name
        )

        glue_client.update_job(
            JobName=job_name,
            JobUpdate=job_config
        )

        print(
            f"Updated Glue job: {job_name}"
        )

    except glue_client.exceptions.EntityNotFoundException:

        glue_client.create_job(
            Name=job_name,
            **job_config
        )

        print(
            f"Created Glue job: {job_name}"
        )


# ============================================================
# CUSTOMERS ETL
# ============================================================

def provision_customers_etl_job():

    create_or_update_job(
        job_name="ecommerce-customers-etl",
        script_location=CUSTOMERS_ETL_SCRIPT
    )


# ============================================================
# CUSTOMERS VALIDATION + QUALITY
# ============================================================

def provision_customers_validation_quality_job():

    create_or_update_job(
        job_name=(
            "ecommerce-customers-"
            "validation-quality"
        ),
        script_location=(
            CUSTOMERS_VALIDATION_QUALITY_SCRIPT
        )
    )


# ============================================================
# CUSTOMERS TRANSFORMATION
# ============================================================

def provision_customers_transformation_job():

    create_or_update_job(
        job_name=(
            "ecommerce-customers-"
            "transformation"
        ),
        script_location=(
            CUSTOMERS_TRANSFORMATION_SCRIPT
        )
    )


# ============================================================
# PRODUCTS ETL
# ============================================================

def provision_products_etl_job():

    create_or_update_job(
        job_name="ecommerce-products-etl",
        script_location=PRODUCTS_ETL_SCRIPT
    )


# ============================================================
# MAIN
# ============================================================

def main():

    print("=" * 70)
    print("Starting Glue Job Provisioning")
    print("=" * 70)

    # --------------------------------------------------------
    # Customers
    # --------------------------------------------------------

    print("\nProvisioning Customers jobs...")

    provision_customers_etl_job()

    provision_customers_validation_quality_job()

    provision_customers_transformation_job()

    # --------------------------------------------------------
    # Products
    # --------------------------------------------------------

    print("\nProvisioning Products jobs...")

    provision_products_etl_job()

    print("\n" + "=" * 70)
    print("Glue Job Provisioning Completed")
    print("=" * 70)


# ============================================================
# ENTRY POINT
# ============================================================

if __name__ == "__main__":
    main()