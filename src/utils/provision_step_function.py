"""Deploy the e-commerce raw-to-refined Step Functions state machine.

This script creates or updates only the state machine named
'ecommerce-raw-layer-pipeline'. It does not start an execution and does not
modify the existing 'ecommerce-customers-pipeline' state machine.

Required runtime dependency: boto3

Expected validation-gate Lambda response (direct JSON object):
    {"status": "PASS", "message": "..."}
Only the exact status PASS allows the refined-layer job to run; all other
statuses fail closed.
"""

from __future__ import annotations

import json
import logging
from typing import Any

import boto3
from botocore.exceptions import ClientError


# ---------------------------------------------------------------------------
# Configuration
# ---------------------------------------------------------------------------
AWS_REGION = "ap-southeast-2"
AWS_ACCOUNT_ID = "256130491261"

STATE_MACHINE_NAME = "ecommerce-raw-layer-pipeline"
STEP_FUNCTION_ROLE_ARN = (
    f"arn:aws:iam::{AWS_ACCOUNT_ID}:role/StepFunctions-EcommerceRole"
)

SOURCE_GENERATION_GLUE_JOB = "ecommerce_generate_source_data"
RAW_LAYER_GLUE_JOB = "ecommerce_raw_layer"
VALIDATION_MAIN_GLUE_JOB = "ecommerce-validation-main"
QUALITY_MAIN_GLUE_JOB = "ecommerce-quality-main"
REFINED_LAYER_GLUE_JOB = "ecommerce-refined-layer"

VALIDATION_GATE_LAMBDA_NAME = "ecommerce-validation-gate"
VALIDATION_GATE_LAMBDA_ARN = (
    f"arn:aws:lambda:{AWS_REGION}:{AWS_ACCOUNT_ID}:"
    f"function:{VALIDATION_GATE_LAMBDA_NAME}"
)

LOG = logging.getLogger("ecommerce-step-functions-deployment")
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(message)s",
)


# ---------------------------------------------------------------------------
# State machine definition
# ---------------------------------------------------------------------------
STATE_MACHINE_DEFINITION: dict[str, Any] = {
    "Comment": (
        "E-Commerce source generation, raw ingestion, validation, quality, "
        "validation gate, and refined-layer pipeline"
    ),
    "StartAt": "Generate Source Data",
    "States": {
        "Generate Source Data": {
            "Type": "Task",
            "Resource": "arn:aws:states:::glue:startJobRun.sync",
            "Parameters": {"JobName": SOURCE_GENERATION_GLUE_JOB},
            "ResultPath": "$.source_generation",
            "Catch": [{
                "ErrorEquals": ["States.ALL"],
                "ResultPath": "$.error",
                "Next": "Source Generation Failed",
            }],
            "Next": "Run Raw Layer",
        },
        "Run Raw Layer": {
            "Type": "Task",
            "Resource": "arn:aws:states:::glue:startJobRun.sync",
            "Parameters": {"JobName": RAW_LAYER_GLUE_JOB},
            "ResultPath": "$.raw_layer",
            "Catch": [{
                "ErrorEquals": ["States.ALL"],
                "ResultPath": "$.error",
                "Next": "Raw Layer Failed",
            }],
            "Next": "Run Validation Main",
        },
        "Run Validation Main": {
            "Type": "Task",
            "Resource": "arn:aws:states:::glue:startJobRun.sync",
            "Parameters": {"JobName": VALIDATION_MAIN_GLUE_JOB},
            "ResultPath": "$.validation",
            "Catch": [{
                "ErrorEquals": ["States.ALL"],
                "ResultPath": "$.error",
                "Next": "Validation Failed",
            }],
            "Next": "Run Quality Main",
        },
        "Run Quality Main": {
            "Type": "Task",
            "Resource": "arn:aws:states:::glue:startJobRun.sync",
            "Parameters": {"JobName": QUALITY_MAIN_GLUE_JOB},
            "ResultPath": "$.quality",
            "Catch": [{
                "ErrorEquals": ["States.ALL"],
                "ResultPath": "$.error",
                "Next": "Quality Failed",
            }],
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
            # Keep the full Lambda result. This avoids a ResultSelector failure
            # if the optional message field is absent. The Choice state below
            # still fails closed unless Payload.status is exactly PASS.
            "ResultPath": "$.validation_gate_result",
            "Catch": [{
                "ErrorEquals": ["States.ALL"],
                "ResultPath": "$.error",
                "Next": "Validation Gate Failed",
            }],
            "Next": "Check Validation Gate",
        },
        "Check Validation Gate": {
            "Type": "Choice",
            "Choices": [{
                "Variable": "$.validation_gate_result.Payload.status",
                "StringEquals": "PASS",
                "Next": "Run Refined Layer",
            }],
            "Default": "Validation Gate Blocked",
        },
        "Run Refined Layer": {
            "Type": "Task",
            "Resource": "arn:aws:states:::glue:startJobRun.sync",
            "Parameters": {"JobName": REFINED_LAYER_GLUE_JOB},
            "ResultPath": "$.refined_layer",
            "Catch": [{
                "ErrorEquals": ["States.ALL"],
                "ResultPath": "$.error",
                "Next": "Refined Layer Failed",
            }],
            "Next": "Pipeline Completed",
        },
        "Pipeline Completed": {"Type": "Succeed"},
        "Source Generation Failed": {
            "Type": "Fail",
            "Error": "SourceGenerationFailed",
            "Cause": (
                "Source dataset generation failed. Check AWS Glue and "
                "CloudWatch logs."
            ),
        },
        "Raw Layer Failed": {
            "Type": "Fail",
            "Error": "RawLayerExecutionFailed",
            "Cause": "Raw-layer ingestion failed. Check AWS Glue logs.",
        },
        "Validation Failed": {
            "Type": "Fail",
            "Error": "ValidationExecutionFailed",
            "Cause": "Dataset validation failed. Check validation reports and Glue logs.",
        },
        "Quality Failed": {
            "Type": "Fail",
            "Error": "QualityExecutionFailed",
            "Cause": "Quality processing failed. Check quality reports and Glue logs.",
        },
        "Validation Gate Failed": {
            "Type": "Fail",
            "Error": "ValidationGateExecutionFailed",
            "Cause": (
                "The validation-gate Lambda invocation failed. Check Lambda "
                "CloudWatch logs and Step Functions execution details."
            ),
        },
        "Validation Gate Blocked": {
            "Type": "Fail",
            "Error": "ValidationGateBlocked",
            "Cause": (
                "The validation gate did not return status PASS. The refined "
                "layer was not started. Check validation and quality reports."
            ),
        },
        "Refined Layer Failed": {
            "Type": "Fail",
            "Error": "RefinedLayerExecutionFailed",
            "Cause": "Refined-layer transformation failed. Check AWS Glue logs.",
        },
    },
}


# ---------------------------------------------------------------------------
# Local definition checks
# ---------------------------------------------------------------------------
def validate_definition(definition: dict[str, Any]) -> None:
    """Catch common ASL wiring errors before sending the definition to AWS."""
    states = definition.get("States", {})
    start = definition.get("StartAt")
    if start not in states:
        raise ValueError(f"StartAt references missing state: {start!r}")

    for state_name, state in states.items():
        next_state = state.get("Next")
        if next_state and next_state not in states:
            raise ValueError(
                f"State {state_name!r} references missing Next state {next_state!r}"
            )
        for catcher in state.get("Catch", []):
            target = catcher.get("Next")
            if target not in states:
                raise ValueError(
                    f"State {state_name!r} Catch references missing state {target!r}"
                )
        for choice in state.get("Choices", []):
            target = choice.get("Next")
            if target not in states:
                raise ValueError(
                    f"Choice state {state_name!r} references missing state {target!r}"
                )
        default = state.get("Default")
        if default and default not in states:
            raise ValueError(
                f"State {state_name!r} Default references missing state {default!r}"
            )

    expected_jobs = {
        SOURCE_GENERATION_GLUE_JOB,
        RAW_LAYER_GLUE_JOB,
        VALIDATION_MAIN_GLUE_JOB,
        QUALITY_MAIN_GLUE_JOB,
        REFINED_LAYER_GLUE_JOB,
    }
    referenced_jobs = {
        state.get("Parameters", {}).get("JobName")
        for state in states.values()
        if state.get("Resource") == "arn:aws:states:::glue:startJobRun.sync"
    }
    if referenced_jobs != expected_jobs:
        raise ValueError(
            "Glue job references do not match configured jobs. "
            f"Expected {sorted(expected_jobs)}, got {sorted(referenced_jobs)}"
        )

    if states["Check Validation Gate"].get("Default") != "Validation Gate Blocked":
        raise ValueError("Validation gate must fail closed for non-PASS responses")

    # Ensure the definition can be serialized before deployment.
    json.dumps(definition)


def verify_glue_jobs(glue_client: Any) -> None:
    required_jobs = [
        SOURCE_GENERATION_GLUE_JOB,
        RAW_LAYER_GLUE_JOB,
        VALIDATION_MAIN_GLUE_JOB,
        QUALITY_MAIN_GLUE_JOB,
        REFINED_LAYER_GLUE_JOB,
    ]
    for job_name in required_jobs:
        try:
            job = glue_client.get_job(JobName=job_name)["Job"]
        except glue_client.exceptions.EntityNotFoundException as exc:
            raise RuntimeError(
                f"Required Glue job {job_name!r} does not exist in "
                f"{AWS_REGION}. Deploy all Glue jobs before the state machine."
            ) from exc
        script_location = job.get("Command", {}).get("ScriptLocation", "<missing>")
        LOG.info("Verified Glue job %s | script=%s", job_name, script_location)


def verify_validation_lambda(lambda_client: Any) -> None:
    try:
        config = lambda_client.get_function_configuration(
            FunctionName=VALIDATION_GATE_LAMBDA_NAME
        )
    except lambda_client.exceptions.ResourceNotFoundException as exc:
        raise RuntimeError(
            f"Required Lambda {VALIDATION_GATE_LAMBDA_NAME!r} does not exist. "
            "Deploy the validation-gate Lambda before the state machine."
        ) from exc

    state = config.get("State")
    if state not in (None, "Active"):
        raise RuntimeError(
            f"Lambda {VALIDATION_GATE_LAMBDA_NAME!r} is not Active; state={state!r}"
        )
    if config.get("Runtime") != "python3.12":
        LOG.warning("Lambda runtime is %s (expected python3.12)", config.get("Runtime"))
    LOG.info(
        "Verified Lambda %s | state=%s | handler=%s | role=%s",
        VALIDATION_GATE_LAMBDA_NAME,
        state or "Active/unspecified",
        config.get("Handler"),
        config.get("Role"),
    )


def find_state_machine(sfn_client: Any) -> str | None:
    paginator = sfn_client.get_paginator("list_state_machines")
    for page in paginator.paginate():
        for machine in page.get("stateMachines", []):
            if machine.get("name") == STATE_MACHINE_NAME:
                return machine["stateMachineArn"]
    return None


def provision_state_machine(sfn_client: Any) -> str:
    definition = json.dumps(STATE_MACHINE_DEFINITION, separators=(",", ":"))
    existing_arn = find_state_machine(sfn_client)

    if existing_arn:
        response = sfn_client.update_state_machine(
            stateMachineArn=existing_arn,
            definition=definition,
            roleArn=STEP_FUNCTION_ROLE_ARN,
        )
        LOG.info("Updated state machine: %s", STATE_MACHINE_NAME)
        LOG.info("Update response: %s", response.get("updateDate"))
        return existing_arn

    response = sfn_client.create_state_machine(
        name=STATE_MACHINE_NAME,
        definition=definition,
        roleArn=STEP_FUNCTION_ROLE_ARN,
        type="STANDARD",
    )
    arn = response["stateMachineArn"]
    LOG.info("Created state machine: %s", STATE_MACHINE_NAME)
    return arn


def main() -> None:
    LOG.info("Starting state machine deployment")
    LOG.info("Region: %s", AWS_REGION)
    LOG.info("State machine: %s", STATE_MACHINE_NAME)

    validate_definition(STATE_MACHINE_DEFINITION)
    LOG.info("Local state-machine definition checks passed")

    session = boto3.Session(region_name=AWS_REGION)
    glue_client = session.client("glue")
    lambda_client = session.client("lambda")
    sfn_client = session.client("stepfunctions")

    try:
        verify_glue_jobs(glue_client)
        verify_validation_lambda(lambda_client)
        arn = provision_state_machine(sfn_client)
    except ClientError:
        LOG.exception("AWS rejected state-machine deployment")
        raise

    LOG.info("State machine ARN: %s", arn)
    LOG.info("Deployment completed successfully; no execution was started")


if __name__ == "__main__":
    main()
