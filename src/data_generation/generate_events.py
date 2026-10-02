import pandas as pd
import numpy as np
import json
from pathlib import Path

# Number of events
N_EVENTS = 5_000_000

# Reproducible random data
RANDOM_SEED = 42

np.random.seed(RANDOM_SEED)

# Generate event IDs
event_ids = np.array([
    f"E{i:09d}"
    for i in range(1, N_EVENTS + 1)
])

# Generate customer IDs
customer_ids = np.array([
    f"C{i:05d}"
    for i in range(1, 10_001)
])

# Assign customers to events
event_customer_ids = np.random.choice(
    customer_ids,
    size=N_EVENTS
)

# Generate event types
event_types = np.random.choice(
    [
        "PRODUCT_VIEWED",
        "ADD_TO_CART",
        "ORDER_CREATED",
        "PAYMENT_COMPLETED",
        "PAYMENT_FAILED",
        "ORDER_SHIPPED",
        "ORDER_DELIVERED"
    ],
    size=N_EVENTS,
    p=[0.35, 0.20, 0.15, 0.12, 0.05, 0.08, 0.05]
)

# Generate order IDs
order_ids = np.array([
    f"O{i:08d}"
    for i in range(1, 1_000_001)
])

# Generate product IDs
product_ids = np.array([
    f"P{i:06d}"
    for i in range(1, 100_001)
])

# Random order assignment
event_order_ids = np.random.choice(
    order_ids,
    size=N_EVENTS
)

# Random product assignment
event_product_ids = np.random.choice(
    product_ids,
    size=N_EVENTS
)

# Events that are associated with an order
order_event_types = [
    "ORDER_CREATED",
    "PAYMENT_COMPLETED",
    "PAYMENT_FAILED",
    "ORDER_SHIPPED",
    "ORDER_DELIVERED"
]

# Create a mask for events that require an order
order_event_mask = np.isin(
    event_types,
    order_event_types
)

# Remove order IDs from events that don't require them
event_order_ids = np.where(
    order_event_mask,
    event_order_ids,
    None
)

# Events that are associated with a product
product_event_types = [
    "PRODUCT_VIEWED",
    "ADD_TO_CART"
]

product_event_mask = np.isin(
    event_types,
    product_event_types
)

event_product_ids = np.where(
    product_event_mask,
    event_product_ids,
    None
)

# Define event timestamp range
start_date = np.datetime64("2025-01-01T00:00:00")
end_date = np.datetime64("2025-12-31T23:59:59")

# Calculate range in seconds
seconds_range = int(
    (end_date - start_date) / np.timedelta64(1, "s")
)

# Generate random event timestamps
random_seconds = np.random.randint(
    0,
    seconds_range + 1,
    size=N_EVENTS
)

event_timestamps = (
    start_date
    + random_seconds.astype("timedelta64[s]")
)

# Generate event source systems
event_sources = np.random.choice(
    [
        "WEB",
        "MOBILE_APP",
        "API",
        "STORE"
    ],
    size=N_EVENTS,
    p=[0.45, 0.35, 0.15, 0.05]
)

# Generate payload data using vectorized NumPy operations

payload_device = np.random.choice(
    ["mobile", "desktop", "tablet"],
    size=N_EVENTS
)

payload_quantity = np.random.randint(
    1,
    6,
    size=N_EVENTS
)

payload_cart_value = np.round(
    np.random.uniform(
        100,
        25000,
        size=N_EVENTS
    ),
    2
)

payload_session_ids = np.array([
    f"S{i:08d}"
    for i in range(1, N_EVENTS + 1)
])

# Step 7: Build Events DataFrame

events_df = pd.DataFrame({
    "event_id": event_ids,
    "event_type": event_types,
    "customer_id": event_customer_ids,
    "order_id": event_order_ids,
    "product_id": event_product_ids,
    "event_timestamp": event_timestamps,
    "source": event_sources,
    "payload": payload_session_ids
})

print("\nEvents Dataset")
print("Rows:", len(events_df))
print("Columns:", len(events_df.columns))

print("\nColumn Names:")
print(events_df.columns.tolist())

print("\nSample Records:")
print(events_df.head())

print("\nEvent Type Distribution:")
print(events_df["event_type"].value_counts())

# Step 8: Basic Validation

print("\nBasic Validation")

print("Duplicate Event IDs:", events_df["event_id"].duplicated().sum())
print("Missing Event IDs:", events_df["event_id"].isna().sum())
print("Missing Customer IDs:", events_df["customer_id"].isna().sum())
print("Missing Event Types:", events_df["event_type"].isna().sum())
print("Missing Timestamps:", events_df["event_timestamp"].isna().sum())

print(
    "Valid Event Types:",
    events_df["event_type"].isin([
        "PRODUCT_VIEWED",
        "ADD_TO_CART",
        "ORDER_CREATED",
        "PAYMENT_COMPLETED",
        "PAYMENT_FAILED",
        "ORDER_SHIPPED",
        "ORDER_DELIVERED"
    ]).all()
)

# Step 9: Save Events Dataset

project_root = Path(__file__).resolve().parents[2]

output_path = project_root / "data" / "events.csv"

events_df.to_csv(
    output_path,
    index=False
)

print("\nEvents dataset saved successfully.")
print("Output path:", output_path)