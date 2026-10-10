
"""
Idempotent deployment for the e-commerce validation-gate Lambda.

Creates missing resources and updates existing resources when needed.
Does not modify any Step Functions state machine.

Requirements:
    pip install boto3

Example:
    python deploy_validation_gate.py --source validation_gate.py
"""

import argparse
import base64
import hashlib
import json
import logging
import time
import zipfile
from io import BytesIO
from pathlib import Path

import boto3
from botocore.exceptions import ClientError


# ============================================================
# CONFIGURATION
# ============================================================

ACCOUNT_ID = "256130491261"
REGION = "ap-southeast-2"
BUCKET = "ecommerce-data-platform-version1"

LAMBDA_NAME = "ecommerce-validation-gate"
LAMBDA_ROLE_NAME = "ecommerce-validation-gate-role"
STEP_FUNCTIONS_ROLE_NAME = "StepFunctions-EcommerceRole"

LAMBDA_RUNTIME = "python3.12"
LAMBDA_TIMEOUT = 60
LAMBDA_MEMORY = 256

BASIC_EXECUTION_POLICY = (
    "arn:aws:iam::aws:policy/service-role/"
    "AWSLambdaBasicExecutionRole"
)

LAMBDA_S3_POLICY_NAME = "EcommerceValidationGateS3ReadPolicy"
STEP_FUNCTIONS_POLICY_NAME = "InvokeEcommerceValidationGatePolicy"

logger = logging.getLogger("ValidationGateDeployment")
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(message)s",
)


# ============================================================
# AWS CLIENTS
# ============================================================

session = boto3.Session(region_name=REGION)
iam = session.client("iam")
lambda_client = session.client("lambda")


# ============================================================
# IAM POLICIES
# ============================================================

def lambda_trust_policy():
    return {
        "Version": "2012-10-17",
        "Statement": [
            {
                "Effect": "Allow",
                "Principal": {"Service": "lambda.amazonaws.com"},
                "Action": "sts:AssumeRole",
            }
        ],
    }


def lambda_s3_policy():
    return {
        "Version": "2012-10-17",
        "Statement": [
            {
                "Sid": "ListValidationAndQualityReports",
                "Effect": "Allow",
                "Action": "s3:ListBucket",
                "Resource": f"arn:aws:s3:::{BUCKET}",
                "Condition": {
                    "StringLike": {
                        "s3:prefix": [
                            "processed/validation",
                            "processed/validation/*",
                            "processed/quality/quality_summary_json",
                            "processed/quality/quality_summary_json/*",
                        ]
                    }
                },
            },
            {
                "Sid": "ReadValidationAndQualityReports",
                "Effect": "Allow",
                "Action": "s3:GetObject",
                "Resource": [
                    f"arn:aws:s3:::{BUCKET}/processed/validation/*",
                    (
                        f"arn:aws:s3:::{BUCKET}/"
                        "processed/quality/quality_summary_json/*"
                    ),
                ],
            },
        ],
    }


def step_functions_invoke_policy():
    return {
        "Version": "2012-10-17",
        "Statement": [
            {
                "Sid": "InvokeEcommerceValidationGate",
                "Effect": "Allow",
                "Action": "lambda:InvokeFunction",
                "Resource": (
                    f"arn:aws:lambda:{REGION}:{ACCOUNT_ID}:"
                    f"function:{LAMBDA_NAME}"
                ),
            }
        ],
    }


# ============================================================
# IAM HELPERS
# ============================================================

def ensure_role(role_name, trust_policy):
    """Create a role if absent; reconcile its trust policy if present."""
    try:
        response = iam.get_role(RoleName=role_name)
        role = response["Role"]

        current_trust = role.get("AssumeRolePolicyDocument", {})
        if current_trust != trust_policy:
            iam.update_assume_role_policy(
                RoleName=role_name,
                PolicyDocument=json.dumps(trust_policy),
            )
            logger.info("Updated trust policy: %s", role_name)
        else:
            logger.info("Trust policy unchanged: %s", role_name)

    except iam.exceptions.NoSuchEntityException:
        response = iam.create_role(
            RoleName=role_name,
            AssumeRolePolicyDocument=json.dumps(trust_policy),
            Description="Execution role for the e-commerce validation gate",
        )
        role = response["Role"]
        logger.info("Created IAM role: %s", role_name)

    return role["Arn"]


def ensure_managed_policy_attached(role_name, policy_arn):
    attached = iam.list_attached_role_policies(
        RoleName=role_name
    )["AttachedPolicies"]

    if any(item["PolicyArn"] == policy_arn for item in attached):
        logger.info("Managed policy already attached: %s", policy_arn)
        return

    iam.attach_role_policy(
        RoleName=role_name,
        PolicyArn=policy_arn,
    )
    logger.info("Attached managed policy: %s", policy_arn)


def ensure_inline_policy(role_name, policy_name, policy_document):
    """Create or update an inline policy with the desired document."""
    desired = json.dumps(policy_document, sort_keys=True)

    try:
        existing = iam.get_role_policy(
            RoleName=role_name,
            PolicyName=policy_name,
        )["PolicyDocument"]

        if json.dumps(existing, sort_keys=True) == desired:
            logger.info(
                "Inline policy unchanged: %s / %s",
                role_name, policy_name,
            )
            return

    except iam.exceptions.NoSuchEntityException:
        pass

    iam.put_role_policy(
        RoleName=role_name,
        PolicyName=policy_name,
        PolicyDocument=json.dumps(policy_document),
    )
    logger.info(
        "Created/updated inline policy: %s / %s",
        role_name, policy_name,
    )


def wait_for_role(role_name, attempts=12):
    """Wait for IAM role visibility after creation."""
    for attempt in range(attempts):
        try:
            return iam.get_role(RoleName=role_name)["Role"]["Arn"]
        except iam.exceptions.NoSuchEntityException:
            time.sleep(2)

    raise RuntimeError(f"IAM role not visible: {role_name}")


# ============================================================
# PACKAGE LAMBDA SOURCE
# ============================================================

def build_deployment_package(source_path):
    """
    Package a Python file or a directory into a Lambda ZIP.

    For a file, it is placed at the ZIP root.
    For a directory, its contents are placed at the ZIP root.
    """
    source = Path(source_path).resolve()

    if not source.exists():
        raise FileNotFoundError(
            f"Lambda source path does not exist: {source}"
        )

    buffer = BytesIO()

    with zipfile.ZipFile(
        buffer, mode="w", compression=zipfile.ZIP_DEFLATED
    ) as archive:
        if source.is_file():
            archive.write(source, arcname=source.name)
        else:
            files = [
                path for path in source.rglob("*")
                if path.is_file()
                and "__pycache__" not in path.parts
                and ".venv" not in path.parts
                and "venv" not in path.parts
            ]

            if not files:
                raise ValueError(f"No files found in {source}")

            for path in files:
                archive.write(
                    path,
                    arcname=path.relative_to(source).as_posix(),
                )

    package = buffer.getvalue()
    code_hash = base64.b64encode(
        hashlib.sha256(package).digest()
    ).decode("ascii")

    return package, code_hash


# ============================================================
# LAMBDA DEPLOYMENT
# ============================================================

def ensure_lambda(source_path, handler, role_arn):
    package, desired_hash = build_deployment_package(source_path)

    if len(package) > 50 * 1024 * 1024:
        raise ValueError(
            "ZIP exceeds Lambda direct-upload limit. "
            "Upload it to S3 and deploy from S3 instead."
        )

    try:
        current = lambda_client.get_function(
            FunctionName=LAMBDA_NAME
        )
        exists = True

    except lambda_client.exceptions.ResourceNotFoundException:
        exists = False
        current = None

    if not exists:
        logger.info("Creating Lambda function: %s", LAMBDA_NAME)

        lambda_client.create_function(
            FunctionName=LAMBDA_NAME,
            Runtime=LAMBDA_RUNTIME,
            Role=role_arn,
            Handler=handler,
            Code={"ZipFile": package},
            Description="Validates e-commerce reports before refined processing",
            Timeout=LAMBDA_TIMEOUT,
            MemorySize=LAMBDA_MEMORY,
            Publish=False,
        )

        lambda_client.get_waiter("function_active_v2").wait(
            FunctionName=LAMBDA_NAME,
            WaiterConfig={"Delay": 2, "MaxAttempts": 60},
        )

    else:
        config = current["Configuration"]

        code_changed = config.get("CodeSha256") != desired_hash

        if code_changed:
            logger.info("Updating Lambda code: %s", LAMBDA_NAME)
            lambda_client.update_function_code(
                FunctionName=LAMBDA_NAME,
                ZipFile=package,
                Publish=False,
            )
            lambda_client.get_waiter("function_updated_v2").wait(
                FunctionName=LAMBDA_NAME,
                WaiterConfig={"Delay": 2, "MaxAttempts": 60},
            )
        else:
            logger.info("Lambda code unchanged; skipping upload")

        # Refresh after code update before checking configuration.
        config = lambda_client.get_function(
            FunctionName=LAMBDA_NAME
        )["Configuration"]

        desired_config = {
            "Role": role_arn,
            "Handler": handler,
            "Runtime": LAMBDA_RUNTIME,
            "Timeout": LAMBDA_TIMEOUT,
            "MemorySize": LAMBDA_MEMORY,
        }

        changed = {
            key: value
            for key, value in desired_config.items()
            if config.get(key) != value
        }

        if changed:
            logger.info("Updating Lambda configuration: %s", changed)
            lambda_client.update_function_configuration(
                FunctionName=LAMBDA_NAME,
                **changed,
            )
            lambda_client.get_waiter("function_updated_v2").wait(
                FunctionName=LAMBDA_NAME,
                WaiterConfig={"Delay": 2, "MaxAttempts": 60},
            )
        else:
            logger.info("Lambda configuration unchanged; skipping")

    arn = lambda_client.get_function(
        FunctionName=LAMBDA_NAME
    )["Configuration"]["FunctionArn"]

    logger.info("Lambda ready: %s", arn)
    return arn


# ============================================================
# MAIN DEPLOYMENT
# ============================================================

def main():
    parser = argparse.ArgumentParser(
        description="Deploy the e-commerce validation-gate Lambda"
    )
    parser.add_argument(
        "--source",
        required=True,
        help="Path to validation_gate.py or a directory containing Lambda code",
    )
    parser.add_argument(
        "--handler",
        default=None,
        help="Handler in module.function format; defaults to <filename>.lambda_handler",
    )
    args = parser.parse_args()

    source = Path(args.source).resolve()

    if source.is_file():
        default_module = source.stem
    else:
        default_module = "validation_gate"

    handler = args.handler or f"{default_module}.lambda_handler"

    logger.info("Starting validation-gate deployment")
    logger.info("Account: %s | Region: %s", ACCOUNT_ID, REGION)

    # 1. Create/reconcile Lambda execution role.
    lambda_role_arn = ensure_role(
        LAMBDA_ROLE_NAME,
        lambda_trust_policy(),
    )

    ensure_managed_policy_attached(
        LAMBDA_ROLE_NAME,
        BASIC_EXECUTION_POLICY,
    )

    ensure_inline_policy(
        LAMBDA_ROLE_NAME,
        LAMBDA_S3_POLICY_NAME,
        lambda_s3_policy(),
    )

    # IAM changes may take a short time to propagate.
    lambda_role_arn = wait_for_role(LAMBDA_ROLE_NAME)
    time.sleep(5)

    # 2. Create or update Lambda code/configuration.
    function_arn = ensure_lambda(
        source_path=source,
        handler=handler,
        role_arn=lambda_role_arn,
    )

    # 3. Ensure the existing Step Functions role can invoke the Lambda.
    try:
        iam.get_role(RoleName=STEP_FUNCTIONS_ROLE_NAME)
    except iam.exceptions.NoSuchEntityException as exc:
        raise RuntimeError(
            f"Expected existing Step Functions role "
            f"{STEP_FUNCTIONS_ROLE_NAME} was not found"
        ) from exc

    ensure_inline_policy(
        STEP_FUNCTIONS_ROLE_NAME,
        STEP_FUNCTIONS_POLICY_NAME,
        step_functions_invoke_policy(),
    )

    logger.info("=" * 65)
    logger.info("VALIDATION GATE DEPLOYMENT COMPLETE")
    logger.info("Function: %s", function_arn)
    logger.info("Handler: %s", handler)
    logger.info("Step Functions invoke permission configured")
    logger.info("No state machine definition was changed")
    logger.info("=" * 65)


if __name__ == "__main__":
    main()
