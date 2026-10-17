
from pyathena import connect

PROFILE = "github-actions-ecommerce"

conn = connect(
    s3_staging_dir=(
        "s3://ecommerce-data-platform-version1/"
        "analytics/athena-results/"
    ),
    region_name="ap-southeast-2",
    schema_name="ecommerce_data_platform",
    work_group="primary",
    profile_name=PROFILE,
)

try:
    cursor = conn.cursor()
    cursor.execute("SELECT * FROM agg_sales_daily LIMIT 10")

    # Print every column returned by Athena.
    columns = [item[0] for item in cursor.description]
    print("\n=== ATHENA COLUMN NAMES ===")
    for index, column in enumerate(columns, start=1):
        print(f"{index}. {column}")

    # Print complete rows without pandas truncation.
    rows = cursor.fetchall()
    print("\n=== SAMPLE DATA ===")
    for row in rows:
        print(dict(zip(columns, row)))

    print(f"\nTotal sample rows: {len(rows)}")

finally:
    conn.close()
