
"""
Provision the E-Commerce Raw-to-Curated Step Functions pipeline.

Pipeline:
    Source Generation
    -> Raw Layer
    -> Validation Main
    -> Quality Main
    -> Validation Gate
    -> Refined Layer
    -> Curated Layer
    -> Pipeline Completed

This script deploys the state machine definition.
It does not start a state machine execution.
"""

from __future__ import annotations

import json
import os
import boto3
from botocore.exceptions import ClientError
from typing import Any


# ============================================================
# CONFIGURATION
# ============================================================

REGION = "ap-southeast-2"

STATE_MACHINE_NAME = "ecommerce-raw-layer-pipeline"

# Use the account that actually owns the state machine and Glue jobs.
# Your recent terminal error showed account 210856421090,
# while earlier configuration referenced 256130491261.
# Verify the correct account before deployment.
ACCOUNT_ID = os.environ.get("AWS_ACCOUNT_ID", "210856421090")

STEP_FUNCTIONS_ROLE_ARN = (
    f"arn:aws:iam::{ACCOUNT_ID}:role/"
    "StepFunctions-EcommerceRole"
)

SOURCE_GENERATION_GLUE_JOB = "ecommerce_generate_source_data"
RAW_LAYER_GLUE_JOB = "ecommerce_raw_layer"
VALIDATION_MAIN_GLUE_JOB = "ecommerce-validation-main"
QUALITY_MAIN_GLUE_JOB = "ecommerce-quality-main"
REFINED_LAYER_GLUE_JOB = "ecommerce-refined-layer"
CURATED_LAYER_GLUE_JOB = "ecommerce-curated-layer"

VALIDATION_GATE_LAMBDA_ARN = (
    f"arn:aws:lambda:{REGION}:{ACCOUNT_ID}:function:"
    "ecommerce-validation-gate"
)

STATE_MACHINE_ARN = (
    f"arn:aws:states:{REGION}:{ACCOUNT_ID}:stateMachine:"
    f"{STATE_MACHINE_NAME}"
)

stepfunctions = boto3.client(
    "stepfunctions",
    region_name=REGION,
)

sts = boto3.client(
    "sts",
    region_name=REGION,
)

iam = boto3.client(
    "iam",
    region_name=REGION,
)


# ============================================================
# STATE MACHINE DEFINITION
# ============================================================

STATE_MACHINE_DEFINITION: dict[str, Any] = {
    "Comment": (
        "E-Commerce source generation, raw ingestion, validation, "
        "quality, validation gate, refined layer, and curated "
        "analytical layer pipeline"
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
                    "Next": "Source Generation Failed",
                }
            ],
            "Next": "Run Raw Layer",
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
                    "Next": "Raw Layer Failed",
                }
            ],
            "Next": "Run Validation Main",
        },

        "Run Validation Main": {
            "Type": "Task",
            "Resource": "arn:aws:states:::glue:startJobRun.sync",
            "Parameters": {
                "JobName": VALIDATION_MAIN_GLUE_JOB
            },
            "ResultPath": "$.validation",
            "Catch": [
                {
                    "ErrorEquals": ["States.ALL"],
                    "ResultPath": "$.error",
                    "Next": "Validation Failed",
                }
            ],
            "Next": "Run Quality Main",
        },

        "Run Quality Main": {
            "Type": "Task",
            "Resource": "arn:aws:states:::glue:startJobRun.sync",
            "Parameters": {
                "JobName": QUALITY_MAIN_GLUE_JOB
            },
            "ResultPath": "$.quality",
            "Catch": [
                {
                    "ErrorEquals": ["States.ALL"],
                    "ResultPath": "$.error",
                    "Next": "Quality Failed",
                }
            ],
            "Next": "Run Validation Gate",
        },

        "Run Validation Gate": {
            "Type": "Task",
            "Resource": "arn:aws:states:::lambda:invoke",
            "Parameters": {
                "FunctionName": VALIDATION_GATE_LAMBDA_ARN,
                "Payload": {
                    "pipeline": STATE_MACHINE_NAME,
                },
            },
            "ResultPath": "$.validation_gate_result",
            "Catch": [
                {
                    "ErrorEquals": ["States.ALL"],
                    "ResultPath": "$.error",
                    "Next": "Validation Gate Failed",
                }
            ],
            "Next": "Check Validation Gate",
        },

        "Check Validation Gate": {
            "Type": "Choice",
            "Choices": [
                {
                    "Variable": (
                        "$.validation_gate_result.Payload.decision"
                    ),
                    "StringEquals": "PASS",
                    "Next": "Run Refined Layer",
                }
            ],
            "Default": "Validation Gate Blocked",
        },

        "Run Refined Layer": {
            "Type": "Task",
            "Resource": "arn:aws:states:::glue:startJobRun.sync",
            "Parameters": {
                "JobName": REFINED_LAYER_GLUE_JOB
            },
            "ResultPath": "$.refined_layer",
            "Catch": [
                {
                    "ErrorEquals": ["States.ALL"],
                    "ResultPath": "$.error",
                    "Next": "Refined Layer Failed",
                }
            ],
            "Next": "Run Curated Layer",
        },

        # NEW: curated processing starts only after refined succeeds.
        "Run Curated Layer": {
            "Type": "Task",
            "Resource": "arn:aws:states:::glue:startJobRun.sync",
            "Parameters": {
                "JobName": CURATED_LAYER_GLUE_JOB
            },
            "ResultPath": "$.curated_layer",
            "TimeoutSeconds": 7200,
            "Retry": [
                {
                    "ErrorEquals": [
                        "Glue.ConcurrentRunsExceededException"
                    ],
                    "IntervalSeconds": 30,
                    "MaxAttempts": 2,
                    "BackoffRate": 2.0,
                }
            ],
            "Catch": [
                {
                    "ErrorEquals": ["States.ALL"],
                    "ResultPath": "$.error",
                    "Next": "Curated Layer Failed",
                }
            ],
            "Next": "Pipeline Completed",
        },

        "Pipeline Completed": {
            "Type": "Succeed"
        },

        "Source Generation Failed": {
            "Type": "Fail",
            "Error": "SourceGenerationFailed",
            "Cause": (
                "Source dataset generation failed. Check AWS Glue "
                "and CloudWatch logs."
            ),
        },

        "Raw Layer Failed": {
            "Type": "Fail",
            "Error": "RawLayerExecutionFailed",
            "Cause": (
                "Raw-layer ingestion failed. Check AWS Glue logs."
            ),
        },

        "Validation Failed": {
            "Type": "Fail",
            "Error": "ValidationExecutionFailed",
            "Cause": (
                "Dataset validation failed. Check validation reports "
                "and Glue logs."
            ),
        },

        "Quality Failed": {
            "Type": "Fail",
            "Error": "QualityExecutionFailed",
            "Cause": (
                "Quality processing failed. Check quality reports "
                "and Glue logs."
            ),
        },

        "Validation Gate Failed": {
            "Type": "Fail",
            "Error": "ValidationGateExecutionFailed",
            "Cause": (
                "The validation-gate Lambda invocation failed. "
                "Check Lambda CloudWatch logs and Step Functions "
                "execution details."
            ),
        },

        "Validation Gate Blocked": {
            "Type": "Fail",
            "Error": "ValidationGateBlocked",
            "Cause": (
                "The validation gate did not return decision PASS. "
                "The refined and curated layers were not started. "
                "Check validation and quality reports."
            ),
        },

        "Refined Layer Failed": {
            "Type": "Fail",
            "Error": "RefinedLayerExecutionFailed",
            "Cause": (
                "Refined-layer transformation failed. "
                "Check AWS Glue logs."
            ),
        },

        "Curated Layer Failed": {
            "Type": "Fail",
            "Error": "CuratedLayerExecutionFailed",
            "Cause": (
                "Curated analytical model processing failed. "
                "Check AWS Glue and CloudWatch logs."
            ),
        },
    },
}


# ============================================================
# VALIDATE DEFINITION LOCALLY
# ============================================================

def validate_definition() -> str:
    definition_json = json.dumps(STATE_MACHINE_DEFINITION)

    # Check that the JSON is serializable.
    json.loads(definition_json)

    # Use the installed boto3/botocore Step Functions model
    # to catch basic request shape problems before deployment.
    stepfunctions.meta.service_model.operation_model(
        "CreateStateMachine"
    )

    print("State machine definition is valid JSON.")
    print("State machine:", STATE_MACHINE_NAME)
    print("Curated Glue job:", CURATED_LAYER_GLUE_JOB)

    return definition_json


# ============================================================
# DEPLOY ONLY THE TARGET STATE MACHINE
# ============================================================

def main() -> None:
    caller = sts.get_caller_identity()
    caller_account = caller["Account"]
    caller_arn = caller["Arn"]

    print("AWS caller:", caller_arn)
    print("Caller account:", caller_account)
    print("Configured account:", ACCOUNT_ID)

    if caller_account != ACCOUNT_ID:
        raise RuntimeError(
            "AWS account mismatch. Set AWS_ACCOUNT_ID to the account "
            "that owns this pipeline, Glue jobs, Lambda and IAM role. "
            "No deployment was performed."
        )

    definition_json = validate_definition()

    # Confirm the curated Glue job exists before deploying a state
    # machine that references it.
    glue = boto3.client("glue", region_name=REGION)
    glue.get_job(JobName=CURATED_LAYER_GLUE_JOB)

    try:
        existing = stepfunctions.describe_state_machine(
            stateMachineArn=STATE_MACHINE_ARN
        )

        # Update only ecommerce-raw-layer-pipeline.
        stepfunctions.update_state_machine(
            stateMachineArn=STATE_MACHINE_ARN,
            definition=definition_json,
            roleArn=STEP_FUNCTIONS_ROLE_ARN,
        )

        print(
            "Updated state machine:",
            existing.get("name", STATE_MACHINE_NAME),
        )

    except stepfunctions.exceptions.StateMachineDoesNotExist:
        stepfunctions.create_state_machine(
            name=STATE_MACHINE_NAME,
            definition=definition_json,
            roleArn=STEP_FUNCTIONS_ROLE_ARN,
            type="STANDARD",
        )

        print("Created state machine:", STATE_MACHINE_NAME)

    print("Deployment complete.")
    print("No state machine execution was started.")


if __name__ == "__main__":
    main()
