import json
import boto3


# =========================================================
# Configuration
# =========================================================

AWS_REGION = "ap-southeast-2"

STATE_MACHINE_NAME = (
    "ecommerce-customers-pipeline"
)

STEP_FUNCTION_ROLE = (
    "arn:aws:iam::256130491261:"
    "role/StepFunctions-EcommerceRole"
)


# =========================================================
# Glue Jobs
# =========================================================

CUSTOMERS_ETL_JOB = (
    "ecommerce-customers-etl"
)

CUSTOMERS_VALIDATION_JOB = (
    "ecommerce-customers-validation-quality"
)

CUSTOMERS_TRANSFORMATION_JOB = (
    "ecommerce-customers-transformation"
)


# =========================================================
# State Machine Definition
# =========================================================

STATE_MACHINE_DEFINITION = {
    "Comment": "E-Commerce Customers ETL Pipeline",

    "StartAt": "Customers ETL",

    "States": {

        "Customers ETL": {
            "Type": "Task",
            "Resource": (
                "arn:aws:states:::glue:startJobRun.sync"
            ),
            "Parameters": {
                "JobName": CUSTOMERS_ETL_JOB
            },
            "Next": "Customers Validation Quality"
        },

        "Customers Validation Quality": {
            "Type": "Task",
            "Resource": (
                "arn:aws:states:::glue:startJobRun.sync"
            ),
            "Parameters": {
                "JobName": CUSTOMERS_VALIDATION_JOB
            },
            "Next": "Customers Transformation"
        },

        "Customers Transformation": {
            "Type": "Task",
            "Resource": (
                "arn:aws:states:::glue:startJobRun.sync"
            ),
            "Parameters": {
                "JobName": CUSTOMERS_TRANSFORMATION_JOB
            },
            "End": True
        }
    }
}


# =========================================================
# Create / Update State Machine
# =========================================================

def provision_state_machine(sfn_client):

    definition = json.dumps(
        STATE_MACHINE_DEFINITION
    )

    response = sfn_client.list_state_machines()

    existing_state_machine = None

    for state_machine in response["stateMachines"]:

        if state_machine["name"] == STATE_MACHINE_NAME:

            existing_state_machine = state_machine

            break

    # -----------------------------------------------------
    # Update existing State Machine
    # -----------------------------------------------------

    if existing_state_machine:

        state_machine_arn = (
            existing_state_machine["stateMachineArn"]
        )

        sfn_client.update_state_machine(
            stateMachineArn=state_machine_arn,
            definition=definition,
            roleArn=STEP_FUNCTION_ROLE
        )

        print(
            f"UPDATED: {STATE_MACHINE_NAME}"
        )

        return state_machine_arn

    # -----------------------------------------------------
    # Create new State Machine
    # -----------------------------------------------------

    response = sfn_client.create_state_machine(
        name=STATE_MACHINE_NAME,
        definition=definition,
        roleArn=STEP_FUNCTION_ROLE,
        type="STANDARD"
    )

    state_machine_arn = response[
        "stateMachineArn"
    ]

    print(
        f"CREATED: {STATE_MACHINE_NAME}"
    )

    return state_machine_arn


# =========================================================
# Main
# =========================================================

def main():

    print("=" * 70)
    print("Step Functions Deployment")
    print("=" * 70)

    print(
        "AWS Region:",
        AWS_REGION
    )

    print(
        "State Machine:",
        STATE_MACHINE_NAME
    )

    session = boto3.Session(
        region_name=AWS_REGION
    )

    sfn_client = session.client(
        "stepfunctions"
    )

    state_machine_arn = provision_state_machine(
        sfn_client
    )

    print(
        "State Machine ARN:",
        state_machine_arn
    )

    print("=" * 70)
    print("Step Functions Deployment Completed")
    print("=" * 70)


if __name__ == "__main__":
    main()