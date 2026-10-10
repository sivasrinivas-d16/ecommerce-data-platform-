
import sys
import logging

from awsglue.utils import getResolvedOptions
from awsglue.context import GlueContext
from awsglue.job import Job
from pyspark.context import SparkContext

from transform_customers import run_transformation as run_customers
from transform_products import run_transformation as run_products
from transform_orders import run_transformation as run_orders
from transform_payments import run_transformation as run_payments
from transform_events import run_transformation as run_events


# ============================================================
# CONFIGURATION
# ============================================================

JOB_NAME = "ecommerce-refined-layer"

TRANSFORMATIONS = [
    ("customers", run_customers),
    ("products", run_products),
    ("orders", run_orders),
    ("payments", run_payments),
    ("events", run_events),
]


# ============================================================
# LOGGING
# ============================================================

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(message)s",
)

logger = logging.getLogger("ECommerceRefinedLayer")


# ============================================================
# SPARK AND GLUE INITIALIZATION
# ============================================================

def create_glue_context():
    sc = SparkContext.getOrCreate()
    glue_context = GlueContext(sc)
    spark = glue_context.spark_session

    return glue_context, spark


# ============================================================
# REFINED TRANSFORMATION ORCHESTRATION
# ============================================================

def run_refined_transformations(glue_context):
    """
    Run all five refined transformations sequentially.

    Each transformation must expose:
        run_transformation(glue_context)

    A genuine execution error stops the pipeline.
    """

    completed = []

    for dataset, transform in TRANSFORMATIONS:
        logger.info("=" * 70)
        logger.info("STARTING REFINED TRANSFORMATION: %s", dataset.upper())
        logger.info("=" * 70)

        try:
            transform(glue_context)

            completed.append(dataset)

            logger.info(
                "REFINED TRANSFORMATION COMPLETED: %s",
                dataset.upper(),
            )

        except Exception:
            logger.exception(
                "REFINED TRANSFORMATION FAILED: %s",
                dataset.upper(),
            )
            raise

    logger.info("=" * 70)
    logger.info("ALL REFINED TRANSFORMATIONS COMPLETED")
    logger.info("Completed datasets: %s", ", ".join(completed))
    logger.info("=" * 70)

    return completed


# ============================================================
# MAIN
# ============================================================

def main():
    args = getResolvedOptions(sys.argv, ["JOB_NAME"])

    glue_context, spark = create_glue_context()

    job = Job(glue_context)
    job.init(args["JOB_NAME"], args)

    try:
        logger.info("E-COMMERCE REFINED LAYER STARTED")

        completed = run_refined_transformations(glue_context)

        if len(completed) != len(TRANSFORMATIONS):
            raise RuntimeError(
                "Not all refined transformations completed successfully."
            )

        job.commit()

        logger.info("E-COMMERCE REFINED LAYER JOB SUCCEEDED")

    except Exception:
        logger.exception("E-COMMERCE REFINED LAYER JOB FAILED")
        raise

    finally:
        spark.stop()
        logger.info("Spark session stopped")


if __name__ == "__main__":
    main()
