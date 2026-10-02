import pandas as pd
import numpy as np
from pathlib import Path

# Number of payment records to generate
N_PAYMENTS = 1_000_000

# Reproducible random data
RANDOM_SEED = 42

np.random.seed(RANDOM_SEED)

# Generate order IDs
order_ids = np.array([
    f"O{i:08d}"
    for i in range(1, N_PAYMENTS + 1)
])

# Generate customer IDs
customer_ids = np.array([
    f"C{i:05d}"
    for i in range(1, 10_001)
])

# Assign a customer to each payment
payment_customer_ids = np.random.choice(
    customer_ids,
    size=N_PAYMENTS
)

# Generate payment methods
payment_methods = np.random.choice(
    [
        "UPI",
        "CREDIT_CARD",
        "DEBIT_CARD",
        "NET_BANKING",
        "WALLET"
    ],
    size=N_PAYMENTS,
    p=[0.40, 0.25, 0.15, 0.12, 0.08]
)

# Generate payment statuses
payment_status = np.random.choice(
    [
        "PAID",
        "PENDING",
        "FAILED",
        "REFUNDED"
    ],
    size=N_PAYMENTS,
    p=[0.78, 0.08, 0.08, 0.06]
)

# Generate payment amounts
payment_amount = np.round(
    np.random.lognormal(
        mean=7.0,
        sigma=1.0,
        size=N_PAYMENTS
    ),
    2
)

# Keep payment amounts within a realistic range
payment_amount = np.clip(
    payment_amount,
    30,
    150000
)

# Generate unique transaction references
transaction_reference = np.array([
    f"TXN{i:012d}"
    for i in range(1, N_PAYMENTS + 1)
])

# Define the payment timestamp range
start_date = np.datetime64("2024-01-01T00:00:00")
end_date = np.datetime64("2025-12-31T23:59:59")

# Calculate the total number of seconds in the date range
seconds_range = int(
    (end_date - start_date) / np.timedelta64(1, "s")
)

# Generate random timestamps
random_seconds = np.random.randint(
    0,
    seconds_range + 1,
    size=N_PAYMENTS
)

payment_timestamps = (
    start_date
    + random_seconds.astype("timedelta64[s]")
)

# Create the payments DataFrame
payments_df = pd.DataFrame({
    "payment_id": [
        f"PAY{i:09d}"
        for i in range(1, N_PAYMENTS + 1)
    ],
    "order_id": order_ids,
    "customer_id": payment_customer_ids,
    "payment_method": payment_methods,
    "payment_status": payment_status,
    "payment_amount": payment_amount,
    "transaction_reference": transaction_reference,
    "payment_timestamp": pd.to_datetime(
        payment_timestamps
    ).strftime("%Y-%m-%d %H:%M:%S")
})

print("Rows:", len(payments_df))
print("Columns:", len(payments_df.columns))

print("\nFirst 5 records:")
print(payments_df.head())

print("\nDuplicate payment IDs:")
print(payments_df["payment_id"].duplicated().sum())

print("\nDuplicate transaction references:")
print(payments_df["transaction_reference"].duplicated().sum())

# Get the project root directory
project_root = Path(__file__).resolve().parents[2]

# Use the existing data folder
output_path = project_root / "data" / "payments.csv"

# Save the dataset
payments_df.to_csv(
    output_path,
    index=False
)

print("\nPayments dataset saved successfully.")
print("Output:", output_path)
print(
    "File size:",
    round(output_path.stat().st_size / (1024 ** 2), 2),
    "MB"
)