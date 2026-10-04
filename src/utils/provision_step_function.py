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

VALIDATION_JOB_NAME = (
    "ecommerce-customers-validation-quality"
)

TRANSFORMATION_JOB_NAME = (
    "ecommerce-customers-transformation"
)


# =========================================================
# Step Functions Definition
# =========================================================

STATE_MACHINE_DEFINITION = {
  "Comment": "E-Commerce Customers ETL Pipeline",
  "StartAt": "Customers ETL",
  "States": {
    "Customers ETL": {
      "Type": "Task",
      "Resource": "arn:aws:states:::glue:startJobRun.sync",
      "Parameters": {
        "JobName": "ecommerce-customers-etl"
      },
      "Next": "Customers Validation Quality"
    },

    "Customers Validation Quality": {
      "Type": "Task",
      "Resource": "arn:aws:states:::glue:startJobRun.sync",
      "Parameters": {
        "JobName": "ecommerce-customers-validation-quality"
      },
      "Next": "Customers Transformation"
    },

    "Customers Transformation": {
      "Type": "Task",
      "Resource": "arn:aws:states:::glue:startJobRun.sync",
      "Parameters": {
        "JobName": "ecommerce-customers-transformation"
      },
      "End": true
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

    try:

        response = sfn_client.list_state_machines()

        existing_state_machine = None

        for machine in response["stateMachines"]:

            if machine["name"] == STATE_MACHINE_NAME:
                existing_state_machine = machine
                break

        if existing_state_machine:

            response = sfn_client.update_state_machine(
                stateMachineArn=(
                    existing_state_machine[
                        "stateMachineArn"
                    ]
                ),
                definition=definition,
                roleArn=STEP_FUNCTION_ROLE
            )

            print(
                "UPDATED:",
                STATE_MACHINE_NAME
            )

            return response["stateMachineArn"]

        response = sfn_client.create_state_machine(
            name=STATE_MACHINE_NAME,
            definition=definition,
            roleArn=STEP_FUNCTION_ROLE,
            type="STANDARD"
        )

        print(
            "CREATED:",
            STATE_MACHINE_NAME
        )

        return response["stateMachineArn"]

    except Exception as error:

        print(
            "Step Functions deployment failed:"
        )

        print(error)

        raise


# =========================================================
# Main
# =========================================================

def main():

    print("=" * 70)
    print("Step Functions Deployment")
    print("=" * 70)

    print(
        "Region:",
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