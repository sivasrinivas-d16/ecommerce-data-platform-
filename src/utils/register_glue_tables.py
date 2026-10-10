
import json
import re
from pathlib import Path

import boto3
from botocore.exceptions import ClientError


# ============================================================
# 1. CONFIGURATION
# ============================================================

AWS_REGION = "ap-southeast-2"
GLUE_DATABASE = "ecommerce_data_platform"
S3_BUCKET = "ecommerce-data-platform-version1"

PROJECT_ROOT = Path(__file__).resolve().parents[2]
SCHEMA_DIR = PROJECT_ROOT / "code"/ "schemas"


# ============================================================
# 2. RAW DATASET -> S3 LOCATION
# ============================================================

DATASET_LOCATIONS = {
    "customers": "raw/customers/",
    "products": "raw/products/",
    "orders": "raw/orders/",
    "payments": "raw/payments/",
    "events": "raw/events/",
}


# ============================================================
# 3. PARQUET TABLE FORMAT
# ============================================================

PARQUET_INPUT_FORMAT = (
    "org.apache.hadoop.hive.ql.io.parquet."
    "MapredParquetInputFormat"
)

PARQUET_OUTPUT_FORMAT = (
    "org.apache.hadoop.hive.ql.io.parquet."
    "MapredParquetOutputFormat"
)

PARQUET_SERDE = (
    "org.apache.hadoop.hive.ql.io.parquet.serde."
    "ParquetHiveSerDe"
)


# ============================================================
# 4. JSON SCHEMA TYPE -> GLUE / ATHENA TYPE
# ============================================================

def convert_type(field):
    """
    Convert a JSON schema field type to a Glue Catalog type.

    Supported examples:
        {"name": "customer_id", "type": "string"}
        {"name": "quantity", "type": "integer"}
        {"name": "signup_date", "type": "date"}
        {"name": "order_timestamp", "type": "timestamp"}
        {"name": "price", "type": "decimal",
         "precision": 12, "scale": 2}

    Decimal fields without explicit precision and scale
    default to decimal(18,2).
    """

    field_type = field.get("type")

    # Also support structured type definitions.
    if isinstance(field_type, dict):
        type_definition = field_type
        field_type = type_definition.get("type")
    else:
        type_definition = field

    if not isinstance(field_type, str):
        raise ValueError(
            f"Invalid type definition for field: {field.get('name')}"
        )

    normalized_type = field_type.lower().strip()

    type_mapping = {
        "string": "string",
        "integer": "int",
        "int": "int",
        "long": "bigint",
        "bigint": "bigint",
        "short": "smallint",
        "boolean": "boolean",
        "float": "float",
        "double": "double",
        "date": "date",
        "timestamp": "timestamp",
        "json": "string",
    }

    if normalized_type in type_mapping:
        return type_mapping[normalized_type]

    if normalized_type == "decimal":
        precision = int(type_definition.get("precision", 18))
        scale = int(type_definition.get("scale", 2))

        if not 1 <= precision <= 38:
            raise ValueError(
                f"Invalid decimal precision {precision} "
                f"for field {field.get('name')}"
            )

        if not 0 <= scale <= precision:
            raise ValueError(
                f"Invalid decimal scale {scale} "
                f"for field {field.get('name')}"
            )

        return f"decimal({precision},{scale})"

    # Support explicitly declared parameterized decimal types.
    match = re.fullmatch(
        r"decimal\s*\(\s*(\d+)\s*,\s*(\d+)\s*\)",
        normalized_type
    )

    if match:
        precision = int(match.group(1))
        scale = int(match.group(2))

        if not 1 <= precision <= 38 or not 0 <= scale <= precision:
            raise ValueError(
                f"Invalid decimal type '{field_type}' "
                f"for field {field.get('name')}"
            )

        return f"decimal({precision},{scale})"

    raise ValueError(
        f"Unsupported schema type '{field_type}' "
        f"for field {field.get('name')}"
    )


# ============================================================
# 5. LOAD AND VALIDATE JSON SCHEMAS
# ============================================================

def load_schema(schema_file):
    """Load a JSON schema and validate its basic structure."""

    with open(schema_file, "r", encoding="utf-8") as file:
        schema = json.load(file)

    if not isinstance(schema, dict):
        raise ValueError(
            f"Schema must be a JSON object: {schema_file}"
        )

    dataset = schema.get("dataset")
    fields = schema.get("fields")

    if not isinstance(dataset, str) or not dataset.strip():
        raise ValueError(
            f"Missing or invalid dataset name: {schema_file}"
        )

    if not isinstance(fields, list) or not fields:
        raise ValueError(
            f"Missing or empty fields list: {schema_file}"
        )

    field_names = []

    for field in fields:
        if not isinstance(field, dict):
            raise ValueError(
                f"Invalid field definition in {schema_file}"
            )

        name = field.get("name")

        if not isinstance(name, str) or not name.strip():
            raise ValueError(
                f"Field with missing name in {schema_file}"
            )

        if name.lower() in [item.lower() for item in field_names]:
            raise ValueError(
                f"Duplicate field name '{name}' in {schema_file}"
            )

        # Validate the field's type before calling AWS.
        convert_type(field)
        field_names.append(name)

    return schema


# ============================================================
# 6. BUILD GLUE CATALOG COLUMNS
# ============================================================

def build_columns(schema):
    """Build Glue column definitions from a JSON schema."""

    columns = []

    for field in schema["fields"]:
        required = field.get("required", False)
        unique = field.get("unique", False)

        columns.append({
            "Name": field["name"],
            "Type": convert_type(field),
            "Comment": (
                f"Required: {required}, Unique: {unique}"
            ),
        })

    return columns


# ============================================================
# 7. BUILD PARQUET TABLE DEFINITION
# ============================================================

def build_parquet_table(
    table_name,
    description,
    s3_location,
    columns
):
    """
    Build an external Glue Catalog table for Parquet files.

    This definition must match the actual Parquet output
    written by the corresponding Glue processor.
    """

    return {
        "Name": table_name,
        "Description": description,
        "TableType": "EXTERNAL_TABLE",

        "Parameters": {
            "classification": "parquet",
            "typeOfData": "file",
        },

        "StorageDescriptor": {
            "Columns": columns,
            "Location": s3_location,
            "InputFormat": PARQUET_INPUT_FORMAT,
            "OutputFormat": PARQUET_OUTPUT_FORMAT,

            "SerdeInfo": {
                "SerializationLibrary": PARQUET_SERDE,
                "Parameters": {},
            },
        },
    }


# ============================================================
# 8. CREATE OR UPDATE A GLUE CATALOG TABLE
# ============================================================

def create_or_update_table(glue_client, table_input):
    """Create a table if missing; otherwise update its definition."""

    table_name = table_input["Name"]

    try:
        glue_client.get_table(
            DatabaseName=GLUE_DATABASE,
            Name=table_name,
        )

    except ClientError as error:
        error_code = error.response.get(
            "Error", {}
        ).get("Code")

        if error_code == "EntityNotFoundException":
            glue_client.create_table(
                DatabaseName=GLUE_DATABASE,
                TableInput=table_input,
            )

            print(f"CREATED: {table_name}")
            return

        raise

    glue_client.update_table(
        DatabaseName=GLUE_DATABASE,
        TableInput=table_input,
    )

    print(f"UPDATED: {table_name}")


# ============================================================
# 9. REGISTER RAW DATASET TABLES
# ============================================================

def register_table(glue_client, schema):
    """Register one raw dataset as a Parquet external table."""

    dataset = schema["dataset"].strip().lower()

    if dataset not in DATASET_LOCATIONS:
        raise ValueError(
            f"No raw S3 location configured for dataset: {dataset}"
        )

    table_name = dataset

    s3_location = (
        f"s3://{S3_BUCKET}/"
        f"{DATASET_LOCATIONS[dataset]}"
    )

    table_input = build_parquet_table(
        table_name=table_name,
        description=schema.get(
            "description",
            f"{dataset.title()} raw Parquet data",
        ),
        s3_location=s3_location,
        columns=build_columns(schema),
    )

    create_or_update_table(
        glue_client=glue_client,
        table_input=table_input,
    )

    print(f"Dataset:  {dataset}")
    print(f"Format:   Parquet")
    print(f"Location: {s3_location}")


# ============================================================
# 10. REGISTER CURATED CUSTOMERS TABLE
# ============================================================

def register_curated_customers(glue_client):
    """
    Register curated customer Parquet data.

    The column definitions must match the actual curated
    output schema produced by the transformation job.
    """

    table_name = "customers_curated"

    columns = [
        {"Name": "customer_id", "Type": "string"},
        {"Name": "name", "Type": "string"},
        {"Name": "email", "Type": "string"},
        {"Name": "city", "Type": "string"},
        {"Name": "state", "Type": "string"},
        {"Name": "country", "Type": "string"},
        {"Name": "signup_date", "Type": "date"},
        {"Name": "signup_year", "Type": "int"},
        {"Name": "signup_month", "Type": "int"},
        {"Name": "signup_day", "Type": "int"},
        {"Name": "tenure_days", "Type": "int"},
        {"Name": "customer_status", "Type": "string"},
    ]

    s3_location = (
        f"s3://{S3_BUCKET}/curated/customers/"
    )

    table_input = build_parquet_table(
        table_name=table_name,
        description=(
            "Curated customer data produced by AWS Glue ETL"
        ),
        s3_location=s3_location,
        columns=columns,
    )

    create_or_update_table(
        glue_client=glue_client,
        table_input=table_input,
    )

    print(f"Curated table location: {s3_location}")


# ============================================================
# 11. MAIN
# ============================================================

def main():
    print("=" * 65)
    print("E-COMMERCE GLUE DATA CATALOG REGISTRATION")
    print("=" * 65)

    print(f"AWS Region : {AWS_REGION}")
    print(f"Database   : {GLUE_DATABASE}")
    print(f"S3 Bucket  : {S3_BUCKET}")
    print(f"Schema Dir : {SCHEMA_DIR}")
    print()

    # Use the configured AWS credentials or execution role.
    session = boto3.Session(
        region_name=AWS_REGION,
    )

    glue_client = session.client("glue")

    # Confirm that the target database exists.
    glue_client.get_database(
        Name=GLUE_DATABASE
    )

    schema_files = sorted(
        SCHEMA_DIR.glob("*.json")
    )

    if not schema_files:
        raise FileNotFoundError(
            f"No JSON schemas found in {SCHEMA_DIR}"
        )

    print(f"Found {len(schema_files)} JSON schema files.")
    print()

    registered_datasets = set()

    for schema_file in schema_files:
        print("-" * 65)
        print(f"Processing schema: {schema_file.name}")

        schema = load_schema(schema_file)
        dataset = schema["dataset"].strip().lower()

        if dataset in registered_datasets:
            raise ValueError(
                f"Duplicate dataset schema detected: {dataset}"
            )

        register_table(
            glue_client=glue_client,
            schema=schema,
        )

        registered_datasets.add(dataset)
        print()

    print("-" * 65)
    print("Registering curated tables...")
    register_curated_customers(glue_client)

    print()
    print("=" * 65)
    print("GLUE DATA CATALOG REGISTRATION COMPLETED")
    print(f"Raw tables registered: {len(registered_datasets)}")
    print("Curated table registered: customers_curated")
    print("=" * 65)


if __name__ == "__main__":
    main()
