
import json
import boto3
from botocore.exceptions import ClientError

# =========================================================
# 1. CONFIGURATION
# =========================================================

AWS_REGION = "ap-southeast-2"
AWS_ACCOUNT_ID = "256130491261"

STATE_MACHINE_NAME = "ecommerce-raw-layer-pipeline"

STEP_FUNCTION_ROLE = (
    f"arn:aws:iam::{AWS_ACCOUNT_ID}:"
    "role/StepFunctions-EcommerceRole"
)

SOURCE_GENERATION_GLUE_JOB = "ecommerce_generate_source_data"
RAW_LAYER_GLUE_JOB = "ecommerce_raw_layer"


# =========================================================
# 2. STATE MACHINE DEFINITION
# =========================================================

STATE_MACHINE_DEFINITION = {
    "Comment": (
        "E-Commerce Source Generation and Raw Layer Pipeline"
    ),
    "StartAt": "Generate Source Data",
    "States": {
        "Generate Source Data": {
            "Type": "Task",
            "Resource": "arn:aws:states:::glue:startJobRun.sync",
            "Parameters": {
                "JobName": SOURCE_GENERATION_GLUE_JOB
            },
            "ResultPath": "$.source_generation",
            "Catch": [
                {
                    "ErrorEquals": ["States.ALL"],
                    "ResultPath": "$.error",
                    "Next": "Source Generation Failed"
                }
            ],
            "Next": "Run Raw Layer"
        },

        "Run Raw Layer": {
            "Type": "Task",
            "Resource": "arn:aws:states:::glue:startJobRun.sync",
            "Parameters": {
                "JobName": RAW_LAYER_GLUE_JOB
            },
            "ResultPath": "$.raw_layer",
            "Catch": [
                {
                    "ErrorEquals": ["States.ALL"],
                    "ResultPath": "$.error",
                    "Next": "Raw Layer Failed"
                }
            ],
            "Next": "Pipeline Completed"
        },

        "Pipeline Completed": {
            "Type": "Succeed"
        },

        "Source Generation Failed": {
            "Type": "Fail",
            "Error": "SourceGenerationFailed",
            "Cause": (
                "Source dataset generation failed. "
                "Check AWS Glue and CloudWatch logs."
            )
        },

        "Raw Layer Failed": {
            "Type": "Fail",
            "Error": "RawLayerExecutionFailed",
            "Cause": (
                "Raw-layer ingestion failed. "
                "Check AWS Glue and CloudWatch logs."
            )
        }
    }
}


# =========================================================
# 3. VERIFY REQUIRED GLUE JOBS
# =========================================================

def verify_glue_jobs(glue_client):
    for job_name in [
        SOURCE_GENERATION_GLUE_JOB,
        RAW_LAYER_GLUE_JOB
    ]:
        try:
            response = glue_client.get_job(JobName=job_name)
            job = response["Job"]

            print(f"Glue job found: {job_name}")
            print(
                "Script location:",
                job["Command"]["ScriptLocation"]
            )

        except glue_client.exceptions.EntityNotFoundException as exc:
            raise RuntimeError(
                f"Required Glue job '{job_name}' does not exist "
                f"in region {AWS_REGION}. "
                "Provision both Glue jobs before deploying "
                "the state machine."
            ) from exc


# =========================================================
# 4. CREATE OR UPDATE STATE MACHINE
# =========================================================

def provision_state_machine(sfn_client):
    definition = json.dumps(STATE_MACHINE_DEFINITION)

    existing_state_machine = None

    paginator = sfn_client.get_paginator("list_state_machines")

    for page in paginator.paginate():
        for state_machine in page["stateMachines"]:
            if state_machine["name"] == STATE_MACHINE_NAME:
                existing_state_machine = state_machine
                break

        if existing_state_machine:
            break

    if existing_state_machine:
        state_machine_arn = (
            existing_state_machine["stateMachineArn"]
        )

        sfn_client.update_state_machine(
            stateMachineArn=state_machine_arn,
            definition=definition,
            roleArn=STEP_FUNCTION_ROLE
        )

        print(f"UPDATED: {STATE_MACHINE_NAME}")

    else:
        response = sfn_client.create_state_machine(
            name=STATE_MACHINE_NAME,
            definition=definition,
            roleArn=STEP_FUNCTION_ROLE,
            type="STANDARD"
        )

        state_machine_arn = response["stateMachineArn"]

        print(f"CREATED: {STATE_MACHINE_NAME}")

    return state_machine_arn


# =========================================================
# 5. MAIN
# =========================================================

def main():
    print("=" * 70)
    print("SOURCE GENERATION + RAW LAYER DEPLOYMENT")
    print("=" * 70)

    print("AWS Region:", AWS_REGION)
    print("State Machine:", STATE_MACHINE_NAME)
    print("Source Generation Job:", SOURCE_GENERATION_GLUE_JOB)
    print("Raw Layer Job:", RAW_LAYER_GLUE_JOB)

    session = boto3.Session(region_name=AWS_REGION)

    glue_client = session.client("glue")
    sfn_client = session.client("stepfunctions")

    # Both jobs must exist before the state machine is deployed.
    verify_glue_jobs(glue_client)

    state_machine_arn = provision_state_machine(sfn_client)

    print("State Machine ARN:", state_machine_arn)
    print("=" * 70)
    print("DEPLOYMENT COMPLETED")
    print("=" * 70)


if __name__ == "__main__":
    main()
