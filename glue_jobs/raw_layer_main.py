import sys
import traceback

from awsglue.context import GlueContext
from awsglue.job import Job
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext

# Import the five reusable raw-layer processors
from ingest_customers import process_customers
from ingest_products import process_products
from ingest_orders import process_orders
from ingest_payments import process_payments
from ingest_events import process_events


# =====================================================
# 1. READ AWS GLUE JOB PARAMETERS
# =====================================================

args = getResolvedOptions(
    sys.argv,
    [
        "JOB_NAME",
        "customers_input_path",
        "customers_output_path",
        "products_input_path",
        "products_output_path",
        "orders_input_path",
        "orders_output_path",
        "payments_input_path",
        "payments_output_path",
        "events_input_path",
        "events_output_path",
        "write_mode",
    ],
)


# =====================================================
# 2. INITIALIZE GLUE AND SPARK ONCE
# =====================================================

sc = SparkContext.getOrCreate()
glue_context = GlueContext(sc)
spark = glue_context.spark_session

job = Job(glue_context)
job.init(args["JOB_NAME"], args)


# =====================================================
# 3. CONFIGURE ALL FIVE DATASETS
# =====================================================

write_mode = args["write_mode"]

datasets = [
    {
        "name": "Customers",
        "processor": process_customers,
        "input_path": args["customers_input_path"],
        "output_path": args["customers_output_path"],
    },
    {
        "name": "Products",
        "processor": process_products,
        "input_path": args["products_input_path"],
        "output_path": args["products_output_path"],
    },
    {
        "name": "Orders",
        "processor": process_orders,
        "input_path": args["orders_input_path"],
        "output_path": args["orders_output_path"],
    },
    {
        "name": "Payments",
        "processor": process_payments,
        "input_path": args["payments_input_path"],
        "output_path": args["payments_output_path"],
    },
    {
        "name": "Events",
        "processor": process_events,
        "input_path": args["events_input_path"],
        "output_path": args["events_output_path"],
    },
]


# =====================================================
# 4. PROCESS ALL FIVE RAW DATASETS SEQUENTIALLY
# =====================================================

try:
    for dataset in datasets:
        print("\n" + "=" * 70)
        print(f"STARTING DATASET: {dataset['name']}")
        print(f"INPUT PATH      : {dataset['input_path']}")
        print(f"OUTPUT PATH     : {dataset['output_path']}")
        print(f"WRITE MODE      : {write_mode}")
        print("=" * 70)

        dataset["processor"](
            spark=spark,
            input_path=dataset["input_path"],
            output_path=dataset["output_path"],
            write_mode=write_mode,
        )

        print(f"SUCCESS: {dataset['name']} completed")

    # Commit only after every dataset has completed successfully.
    job.commit()

    print("\n" + "=" * 70)
    print("SUCCESS: ALL FIVE RAW DATASETS COMPLETED")
    print("=" * 70)

except Exception:
    print("\nERROR: RAW LAYER FAILED")
    traceback.print_exc()
    raise

finally:
    print("Raw-layer Glue job execution finished.")
