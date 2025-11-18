import pyarrow.parquet as pq
import pandas as pd
import numpy as np
import json

def to_json_safe(value):
    """Recursively convert any value to something JSON can serialize."""

    # None is fine
    if value is None:
        return None

    # pandas timestamp
    if isinstance(value, pd.Timestamp):
        return value.isoformat()

    # numpy scalars
    if isinstance(value, (np.integer, np.floating, np.bool_)):
        return value.item()

    # numpy array → list
    if isinstance(value, np.ndarray):
        return [to_json_safe(v) for v in value.tolist()]

    # bytes → list[int]
    if isinstance(value, (bytes, bytearray)):
        return list(value)

    # list → convert items recursively
    if isinstance(value, list):
        return [to_json_safe(v) for v in value]

    # tuple → convert items recursively
    if isinstance(value, tuple):
        return [to_json_safe(v) for v in value]

    # dict → convert values
    if isinstance(value, dict):
        return {k: to_json_safe(v) for k, v in value.items()}

    # int/float/bool/str → safe
    if isinstance(value, (int, float, bool, str)):
        return value

    # fallback: string
    return str(value)


# Load parquet
table = pq.read_table("data.parquet")
df = table.to_pandas()

# Convert rows
safe_records = []
for _, row in df.iterrows():
    safe_row = {col: to_json_safe(val) for col, val in row.items()}
    safe_records.append(safe_row)

# Write output
with open("data.json", "w") as f:
    json.dump(safe_records, f, indent=2)
