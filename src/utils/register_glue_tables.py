import json
from pathlib import Path

import boto3


# ---------------------------------------------------------
# Configuration
# ---------------------------------------------------------

AWS_REGION = "ap-southeast-2"
GLUE_DATABASE = "ecommerce_data_platform"
S3_BUCKET = "ecommerce-data-platform-version1"

PROJECT_ROOT = Path(__file__).resolve().parents[2]
SCHEMA_DIR = PROJECT_ROOT / "schemas"


# ---------------------------------------------------------
# Dataset → S3 location
# ---------------------------------------------------------

DATASET_LOCATIONS = {
    "customers": "raw/customers/",
    "products": "raw/products/",
    "orders": "raw/orders/",
    "payments": "raw/payments/",
    "events": "raw/events/",
}


# ---------------------------------------------------------
# JSON schema type → Glue/Athena type
# ---------------------------------------------------------

def convert_type(field_type):
    type_mapping = {
        "string": "string",
        "integer": "int",
        "date": "string",
        "timestamp": "timestamp",
        "json": "string",
        "decimal": "decimal(18,2)",
    }

    return type_mapping.get(field_type.lower(), "string")


# ---------------------------------------------------------
# Load JSON schema
# ---------------------------------------------------------

def load_schema(schema_file):
    with open(schema_file, "r", encoding="utf-8") as file:
        return json.load(file)


# ---------------------------------------------------------
# Build Glue columns
# ---------------------------------------------------------

def build_columns(schema):
    columns = []

    for field in schema["fields"]:
        columns.append(
            {
                "Name": field["name"],
                "Type": convert_type(field["type"]),
                "Comment": (
                    f"Required: {field.get('required', False)}, "
                    f"Unique: {field.get('unique', False)}"
                ),
            }
        )

    return columns


# ---------------------------------------------------------
# Create / update Glue table
# ---------------------------------------------------------

def register_table(glue_client, schema):

    dataset = schema["dataset"]

    if dataset not in DATASET_LOCATIONS:
        raise ValueError(
            f"No S3 location configured for dataset: {dataset}"
        )

    table_name = dataset

    s3_location = (
        f"s3://{S3_BUCKET}/"
        f"{DATASET_LOCATIONS[dataset]}"
    )

    columns = build_columns(schema)

    table_input = {
        "Name": table_name,
        "Description": schema.get(
            "description",
            f"{dataset} data"
        ),
        "TableType": "EXTERNAL_TABLE",

        "Parameters": {
            "classification": "csv",
            "skip.header.line.count": "1",
            "typeOfData": "file",
        },

        "StorageDescriptor": {
            "Columns": columns,

            "Location": s3_location,

            "InputFormat": (
                "org.apache.hadoop.mapred.TextInputFormat"
            ),

            "OutputFormat": (
                "org.apache.hadoop.hive.ql.io."
                "HiveIgnoreKeyTextOutputFormat"
            ),

            "SerdeInfo": {
                "SerializationLibrary": (
                    "org.apache.hadoop.hive.serde2.OpenCSVSerde"
                ),
                "Parameters": {
                    "separatorChar": ",",
                    "quoteChar": '"',
                },
            },
        },
    }

    try:
        glue_client.get_table(
            DatabaseName=GLUE_DATABASE,
            Name=table_name,
        )

        glue_client.update_table(
            DatabaseName=GLUE_DATABASE,
            TableInput=table_input,
        )

        print(f"UPDATED: {table_name}")

    except glue_client.exceptions.EntityNotFoundException:

        glue_client.create_table(
            DatabaseName=GLUE_DATABASE,
            TableInput=table_input,
        )

        print(f"CREATED: {table_name}")


# ---------------------------------------------------------
# Main
# ---------------------------------------------------------

def main():

    print("=" * 60)
    print("Glue Data Catalog Registration")
    print("=" * 60)

    print(f"AWS Region : {AWS_REGION}")
    print(f"Database   : {GLUE_DATABASE}")
    print(f"Schema Dir : {SCHEMA_DIR}")
    print()

    session = boto3.Session(
        profile_name="github-actions-ecommerce",
        region_name=AWS_REGION,
    )

    glue_client = session.client("glue")

    schema_files = sorted(
        SCHEMA_DIR.glob("*.json")
    )

    if not schema_files:
        raise FileNotFoundError(
            f"No JSON schemas found in {SCHEMA_DIR}"
        )

    print(
        f"Found {len(schema_files)} schema files"
    )
    print()

    for schema_file in schema_files:

        print(
            f"Processing: {schema_file.name}"
        )

        schema = load_schema(schema_file)

        register_table(
            glue_client,
            schema,
        )

        print()

    print("=" * 60)
    print("Glue table registration completed")
    print("=" * 60)


if __name__ == "__main__":
    main()