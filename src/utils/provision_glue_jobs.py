import boto3


# =========================================================
# Configuration
# =========================================================

AWS_REGION = "ap-southeast-2"

GLUE_ROLE = "AWSGlueServiceRole-ecommerce"

CUSTOMERS_JOB_NAME = "ecommerce-customers-validation-quality"

CUSTOMERS_SCRIPT_LOCATION = (
    "s3://ecommerce-data-platform-version1/"
    "code/src/validation_quality/"
    "glue_customers_validation_quality.py"
)


# =========================================================
# Create / Update Glue Job
# =========================================================

def provision_customers_job(glue_client):

    job_command = {
        "Name": "glueetl",
        "ScriptLocation": CUSTOMERS_SCRIPT_LOCATION,
        "PythonVersion": "3"
    }

    job_arguments = {
        "--job-language": "python",
        "--enable-metrics": "true",
        "--enable-continuous-cloudwatch-log": "true",
        "--enable-spark-ui": "true",
        "--spark-event-logs-path":
            "s3://ecommerce-data-platform-version1/"
            "processed/spark-events/"
    }

    job_input = {
        "Name": CUSTOMERS_JOB_NAME,
        "Role": GLUE_ROLE,
        "ExecutionProperty": {
            "MaxConcurrentRuns": 1
        },
        "Command": job_command,
        "DefaultArguments": job_arguments,
        "GlueVersion": "5.1",
        "WorkerType": "G.1X",
        "NumberOfWorkers": 2,
        "Timeout": 15,
        "MaxRetries": 0
    }

    try:

        glue_client.get_job(
            JobName=CUSTOMERS_JOB_NAME
        )

        print(
            f"Glue job exists: {CUSTOMERS_JOB_NAME}"
        )

        glue_client.update_job(
            JobName=CUSTOMERS_JOB_NAME,
            JobUpdate={
                key: value
                for key, value in job_input.items()
                if key != "Name"
            }
        )

        print(
            f"UPDATED: {CUSTOMERS_JOB_NAME}"
        )

    except glue_client.exceptions.EntityNotFoundException:

        glue_client.create_job(
            **job_input
        )

        print(
            f"CREATED: {CUSTOMERS_JOB_NAME}"
        )


# =========================================================
# Start Glue Job
# =========================================================

def start_customers_job(glue_client):

    response = glue_client.start_job_run(
        JobName=CUSTOMERS_JOB_NAME
    )

    run_id = response["JobRunId"]

    print(
        f"STARTED: {CUSTOMERS_JOB_NAME}"
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

    # Create or update the Glue job
    provision_customers_job(
        glue_client
    )

    # Automatically start the Glue job
    start_customers_job(
        glue_client
    )

    print("=" * 70)
    print("Glue Job Provisioning Completed")
    print("=" * 70)


if __name__ == "__main__":
    main()