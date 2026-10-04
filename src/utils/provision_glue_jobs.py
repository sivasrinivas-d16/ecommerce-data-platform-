import boto3


# =========================================================
# Configuration
# =========================================================

AWS_REGION = "ap-southeast-2"

GLUE_ROLE = "AWSGlueServiceRole-ecommerce"


# =========================================================
# Customer Validation + Quality
# =========================================================

CUSTOMERS_VALIDATION_JOB_NAME = (
    "ecommerce-customers-validation-quality"
)

CUSTOMERS_VALIDATION_SCRIPT_LOCATION = (
    "s3://ecommerce-data-platform-version1/"
    "code/src/validation_quality/"
    "glue_customers_validation_quality.py"
)


# =========================================================
# Customer Transformation
# =========================================================

CUSTOMERS_TRANSFORMATION_JOB_NAME = (
    "ecommerce-customers-transformation"
)

CUSTOMERS_TRANSFORMATION_SCRIPT_LOCATION = (
    "s3://ecommerce-data-platform-version1/"
    "code/src/transformation/"
    "transform_customers.py"
)


# =========================================================
# Common Glue Job Configuration
# =========================================================

def get_job_command(script_location):

    return {
        "Name": "glueetl",
        "ScriptLocation": script_location,
        "PythonVersion": "3"
    }


def get_job_arguments():

    return {
        "--job-language": "python",
        "--enable-metrics": "true",
        "--enable-continuous-cloudwatch-log": "true",
        "--enable-spark-ui": "true",
        "--spark-event-logs-path":
            "s3://ecommerce-data-platform-version1/"
            "processed/spark-events/"
    }


# =========================================================
# Create / Update Customer Validation + Quality Job
# =========================================================

def provision_customers_validation_job(glue_client):

    job_input = {
        "Name": CUSTOMERS_VALIDATION_JOB_NAME,
        "Role": GLUE_ROLE,
        "ExecutionProperty": {
            "MaxConcurrentRuns": 1
        },
        "Command": get_job_command(
            CUSTOMERS_VALIDATION_SCRIPT_LOCATION
        ),
        "DefaultArguments": get_job_arguments(),
        "GlueVersion": "5.1",
        "WorkerType": "G.1X",
        "NumberOfWorkers": 2,
        "Timeout": 15,
        "MaxRetries": 0
    }

    try:

        glue_client.get_job(
            JobName=CUSTOMERS_VALIDATION_JOB_NAME
        )

        print(
            f"Glue job exists: "
            f"{CUSTOMERS_VALIDATION_JOB_NAME}"
        )

        glue_client.update_job(
            JobName=CUSTOMERS_VALIDATION_JOB_NAME,
            JobUpdate={
                key: value
                for key, value in job_input.items()
                if key != "Name"
            }
        )

        print(
            f"UPDATED: "
            f"{CUSTOMERS_VALIDATION_JOB_NAME}"
        )

    except glue_client.exceptions.EntityNotFoundException:

        glue_client.create_job(
            **job_input
        )

        print(
            f"CREATED: "
            f"{CUSTOMERS_VALIDATION_JOB_NAME}"
        )


# =========================================================
# Create / Update Customer Transformation Job
# =========================================================

def provision_customers_transformation_job(glue_client):

    job_input = {
        "Name": CUSTOMERS_TRANSFORMATION_JOB_NAME,
        "Role": GLUE_ROLE,
        "ExecutionProperty": {
            "MaxConcurrentRuns": 1
        },
        "Command": get_job_command(
            CUSTOMERS_TRANSFORMATION_SCRIPT_LOCATION
        ),
        "DefaultArguments": get_job_arguments(),
        "GlueVersion": "5.1",
        "WorkerType": "G.1X",
        "NumberOfWorkers": 2,
        "Timeout": 15,
        "MaxRetries": 0
    }

    try:

        glue_client.get_job(
            JobName=CUSTOMERS_TRANSFORMATION_JOB_NAME
        )

        print(
            f"Glue job exists: "
            f"{CUSTOMERS_TRANSFORMATION_JOB_NAME}"
        )

        glue_client.update_job(
            JobName=CUSTOMERS_TRANSFORMATION_JOB_NAME,
            JobUpdate={
                key: value
                for key, value in job_input.items()
                if key != "Name"
            }
        )

        print(
            f"UPDATED: "
            f"{CUSTOMERS_TRANSFORMATION_JOB_NAME}"
        )

    except glue_client.exceptions.EntityNotFoundException:

        glue_client.create_job(
            **job_input
        )

        print(
            f"CREATED: "
            f"{CUSTOMERS_TRANSFORMATION_JOB_NAME}"
        )


# =========================================================
# Start Customer Validation + Quality Job
# =========================================================

def start_customers_validation_job(glue_client):

    response = glue_client.start_job_run(
        JobName=CUSTOMERS_VALIDATION_JOB_NAME
    )

    run_id = response["JobRunId"]

    print(
        f"STARTED: "
        f"{CUSTOMERS_VALIDATION_JOB_NAME}"
    )

    print(
        f"Glue Job Run ID: {run_id}"
    )

    return run_id


# =========================================================
# Start Customer Transformation Job
# =========================================================

def start_customers_transformation_job(glue_client):

    response = glue_client.start_job_run(
        JobName=CUSTOMERS_TRANSFORMATION_JOB_NAME
    )

    run_id = response["JobRunId"]

    print(
        f"STARTED: "
        f"{CUSTOMERS_TRANSFORMATION_JOB_NAME}"
    )

    print(
        f"Glue Job Run ID: {run_id}"
    )

    return run_id


# =========================================================
# Main
# =========================================================

def main():

    print("=" * 70)
    print("Glue Job Provisioning")
    print("=" * 70)

    print(
        "AWS Region:",
        AWS_REGION
    )

    print(
        "Glue Role:",
        GLUE_ROLE
    )

    # GitHub Actions provides AWS credentials
    # through environment variables.
    #
    # Do NOT specify profile_name here.

    session = boto3.Session(
        region_name=AWS_REGION
    )

    glue_client = session.client(
        "glue"
    )

    # -----------------------------------------------------
    # Validation + Quality
    # -----------------------------------------------------

    provision_customers_validation_job(
        glue_client
    )

    # start_customers_validation_job(
    #     glue_client
    # )

    # -----------------------------------------------------
    # Transformation
    # -----------------------------------------------------

    provision_customers_transformation_job(
        glue_client
    )

    # start_customers_transformation_job(
    #     glue_client
    # )

    print("=" * 70)
    print("Glue Job Provisioning Completed")
    print("=" * 70)


if __name__ == "__main__":
    main()