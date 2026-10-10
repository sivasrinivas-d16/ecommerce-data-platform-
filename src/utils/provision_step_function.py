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

RAW_LAYER_GLUE_JOB = "ecommerce_raw_layer"


# =========================================================
# 2. STATE MACHINE DEFINITION
# =========================================================

STATE_MACHINE_DEFINITION = {
    "Comment": (
        "E-Commerce Raw Layer Pipeline - "
        "Customers, Products, Orders, Payments and Events"
    ),
    "StartAt": "Run Raw Layer",
    "States": {
        "Run Raw Layer": {
            "Type": "Task",
            "Resource": "arn:aws:states:::glue:startJobRun.sync",
            "Parameters": {
                "JobName": RAW_LAYER_GLUE_JOB
            },
            "Catch": [
                {
                    "ErrorEquals": ["States.ALL"],
                    "ResultPath": "$.error",
                    "Next": "Raw Layer Failed"
                }
            ],
            "Next": "Raw Layer Completed"
        },
        "Raw Layer Completed": {
            "Type": "Succeed"
        },
        "Raw Layer Failed": {
            "Type": "Fail",
            "Error": "RawLayerExecutionFailed",
            "Cause": (
                "The combined raw-layer Glue job failed. "
                "Check the Glue and CloudWatch logs."
            )
        }
    }
}


# =========================================================
# 3. VERIFY GLUE JOB
# =========================================================

def verify_glue_job(glue_client):
    try:
        response = glue_client.get_job(
            JobName=RAW_LAYER_GLUE_JOB
        )

        job = response["Job"]
        script_location = job["Command"]["ScriptLocation"]

        print(f"Glue job found: {RAW_LAYER_GLUE_JOB}")
        print(f"Script location: {script_location}")

        if not script_location:
            raise RuntimeError(
                "The Glue job has no configured script location."
            )

    except glue_client.exceptions.EntityNotFoundException as exc:
        raise RuntimeError(
            f"Glue job '{RAW_LAYER_GLUE_JOB}' does not exist "
            f"in AWS region {AWS_REGION}. "
            "Run the Glue job provisioning script first."
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
    print("STEP FUNCTIONS DEPLOYMENT")
    print("=" * 70)

    print("AWS Region:", AWS_REGION)
    print("State Machine:", STATE_MACHINE_NAME)
    print("Glue Job:", RAW_LAYER_GLUE_JOB)

    session = boto3.Session(region_name=AWS_REGION)

    glue_client = session.client("glue")
    sfn_client = session.client("stepfunctions")

    # Confirm the Glue job exists before deploying its orchestrator.
    verify_glue_job(glue_client)

    state_machine_arn = provision_state_machine(sfn_client)

    print("State Machine ARN:", state_machine_arn)

    print("=" * 70)
    print("STEP FUNCTIONS DEPLOYMENT COMPLETED")
    print("=" * 70)


if __name__ == "__main__":
    main()
